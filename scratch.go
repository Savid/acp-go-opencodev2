package opencodeacp

import "github.com/savid/acp-go-core/process"

// scratchDir creates one ephemeral directory for one purpose under the
// configured scratch parent.
func (a *Agent) scratchDir(purpose string) (string, error) {
	return process.ScratchDir(a.options.ScratchDir, vendor, purpose)
}
