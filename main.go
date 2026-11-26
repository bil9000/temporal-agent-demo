// A small agent run on Temporal, after Simon's legal-docs example.
// Each step is one call to a local model; each call sees the contract and every earlier answer.
// If the worker dies mid-step, Temporal retries that step only. Earlier answers come from history.
//
//	agent worker                 run the worker
//	agent start <input.json> <id> start a run
//	agent state <id>             print how many steps are done
//	agent result <id>            wait for the run and print its result as JSON
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

const taskQueue = "agent"

type RunInput struct {
	Document  string   `json:"document"`
	Steps     []string `json:"steps"`
	MaxTokens int      `json:"maxTokens"` // stop before the next step once this many tokens are spent; 0 = no limit
}

type StepResult struct {
	Step         string  `json:"step"`
	Answer       string  `json:"answer"`
	Attempt      int32   `json:"attempt"`
	PromptTokens int     `json:"promptTokens"`
	OutputTokens int     `json:"outputTokens"`
	Seconds      float64 `json:"seconds"`
}

type RunState struct {
	Done       []StepResult `json:"done"`
	TokensUsed int          `json:"tokensUsed"`
	Stopped    string       `json:"stopped,omitempty"`
}

func AgentRun(ctx workflow.Context, in RunInput) (RunState, error) {
	var st RunState
	if err := workflow.SetQueryHandler(ctx, "state", func() (RunState, error) { return st, nil }); err != nil {
		return st, err
	}
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute,
		HeartbeatTimeout:    15 * time.Second, // a dead worker is noticed in ~15 s, not at the 10-minute timeout
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
	})
	for _, step := range in.Steps {
		if in.MaxTokens > 0 && st.TokensUsed >= in.MaxTokens {
			st.Stopped = fmt.Sprintf("budget of %d tokens reached before %q", in.MaxTokens, step)
			return st, nil
		}
		var r StepResult
		if err := workflow.ExecuteActivity(ctx, CallModel, step, buildPrompt(in.Document, st.Done, step)).Get(ctx, &r); err != nil {
			return st, err
		}
		st.Done = append(st.Done, r)
		st.TokensUsed += r.PromptTokens + r.OutputTokens
	}
	return st, nil
}

func buildPrompt(doc string, done []StepResult, step string) string {
	var b strings.Builder
	b.WriteString("You are reviewing this contract. Answer in at most two short sentences.\n\nCONTRACT:\n")
	b.WriteString(doc)
	for _, d := range done {
		fmt.Fprintf(&b, "\n\nQ: %s\nA: %s", d.Step, d.Answer)
	}
	fmt.Fprintf(&b, "\n\nQ: %s\nA:", step)
	return b.String()
}

// CallModel makes one streamed call to the model and heartbeats while it waits,
// so Temporal can tell a slow model from a dead worker.
func CallModel(ctx context.Context, step, prompt string) (StepResult, error) {
	info := activity.GetInfo(ctx)
	start := time.Now()
	log.Printf("step %q attempt %d: calling model", step, info.Attempt)

	stop := make(chan struct{})
	defer close(stop)
	go func() {
		t := time.NewTicker(3 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				activity.RecordHeartbeat(ctx)
			}
		}
	}()

	body, _ := json.Marshal(map[string]any{
		"model":   env("MODEL", "qwen3-coder:30b"),
		"prompt":  prompt,
		"stream":  true,
		"options": map[string]any{"num_predict": 60, "temperature": 0},
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, env("OLLAMA_URL", "http://host.docker.internal:11434")+"/api/generate", bytes.NewReader(body))
	if err != nil {
		return StepResult{}, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return StepResult{}, err
	}
	defer resp.Body.Close()

	r := StepResult{Step: step, Attempt: info.Attempt}
	var answer strings.Builder
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		var c struct {
			Response        string `json:"response"`
			Done            bool   `json:"done"`
			PromptEvalCount int    `json:"prompt_eval_count"`
			EvalCount       int    `json:"eval_count"`
			Error           string `json:"error"`
		}
		if err := json.Unmarshal(sc.Bytes(), &c); err != nil {
			return r, err
		}
		if c.Error != "" {
			return r, fmt.Errorf("model: %s", c.Error)
		}
		answer.WriteString(c.Response)
		if c.Done {
			r.PromptTokens, r.OutputTokens = c.PromptEvalCount, c.EvalCount
		}
	}
	if err := sc.Err(); err != nil {
		return r, err
	}
	r.Answer = strings.TrimSpace(answer.String())
	r.Seconds = time.Since(start).Seconds()
	log.Printf("step %q attempt %d: done, %d in + %d out tokens, %.0fs", step, info.Attempt, r.PromptTokens, r.OutputTokens, r.Seconds)
	return r, nil
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func main() {
	c, err := client.Dial(client.Options{HostPort: env("TEMPORAL_ADDRESS", "temporal:7233")})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()
	ctx := context.Background()

	cmd := "worker"
	if len(os.Args) > 1 {
		cmd = os.Args[1]
	}
	switch cmd {
	case "start":
		raw, err := os.ReadFile(os.Args[2])
		if err != nil {
			log.Fatal(err)
		}
		var in RunInput
		if err := json.Unmarshal(raw, &in); err != nil {
			log.Fatal(err)
		}
		run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: os.Args[3], TaskQueue: taskQueue}, AgentRun, in)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(run.GetID())
	case "state":
		v, err := c.QueryWorkflow(ctx, os.Args[2], "", "state")
		if err != nil {
			log.Fatal(err)
		}
		var st RunState
		if err := v.Get(&st); err != nil {
			log.Fatal(err)
		}
		fmt.Println(len(st.Done))
	case "result":
		var st RunState
		if err := c.GetWorkflow(ctx, os.Args[2], "").Get(ctx, &st); err != nil {
			log.Fatal(err)
		}
		out, _ := json.MarshalIndent(st, "", "  ")
		fmt.Println(string(out))
	default:
		w := worker.New(c, taskQueue, worker.Options{})
		w.RegisterWorkflow(AgentRun)
		w.RegisterActivity(CallModel)
		if err := w.Run(worker.InterruptCh()); err != nil {
			log.Fatal(err)
		}
	}
}
