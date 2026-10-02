package opencodeacp_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"

	opencodeacp "github.com/savid/acp-go-opencodev2"
)

func ExampleNewOpenCodeOptions() {
	options := opencodeacp.NewOpenCodeOptions(
		opencodeacp.WithOpenCodeModel("provider/model"),
		opencodeacp.WithOpenCodeEffort("high"),
	)

	fmt.Println(options.Model)
	fmt.Println(options.Effort)
	// Output:
	// provider/model
	// high
}

// ExampleServe_initialize embeds the agent over a pair of pipes, the same
// wiring a host uses for stdio, and reads the capabilities the handshake
// advertises.
func ExampleServe_initialize() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clientToAgentReader, clientToAgentWriter := io.Pipe()
	agentToClientReader, agentToClientWriter := io.Pipe()

	done := make(chan error, 1)

	go func() {
		done <- opencodeacp.Serve(ctx, clientToAgentReader, agentToClientWriter)
	}()

	_, _ = fmt.Fprintln(clientToAgentWriter,
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1}}`)

	line, _ := bufio.NewReader(agentToClientReader).ReadString('\n')

	cancel()
	_ = clientToAgentWriter.Close()
	<-done

	var response struct {
		Result struct {
			AuthMethods       []any `json:"authMethods"`
			AgentCapabilities struct {
				LoadSession         bool           `json:"loadSession"`
				SessionCapabilities map[string]any `json:"sessionCapabilities"`
			} `json:"agentCapabilities"`
		} `json:"result"`
	}

	_ = json.Unmarshal([]byte(line), &response)

	fmt.Println(len(response.Result.AuthMethods))
	fmt.Println(response.Result.AgentCapabilities.LoadSession)
	fmt.Println(len(response.Result.AgentCapabilities.SessionCapabilities))
	// Output:
	// 0
	// true
	// 5
}
