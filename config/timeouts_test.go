package config

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestSandboxAppStartPolicyRoundTripAndValidation(t *testing.T) {
	c, err := DecodeConductor(strings.NewReader("sandbox: {timeouts: {app_start: 3s}}"))
	if err != nil {
		t.Fatal(err)
	}
	if c.Sandbox.Timeouts.AppStart != "3s" || c.Clone().Sandbox.Timeouts.AppStart != "3s" {
		t.Fatal(c.Sandbox.Timeouts)
	}
	raw, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	var round Conductor
	if err := json.Unmarshal(raw, &round); err != nil || round.Sandbox.Timeouts.Runtime().AppStart != "3s" {
		t.Fatal(err, round.Sandbox.Timeouts)
	}
	for _, input := range []string{"-1s", "bad"} {
		if _, err := DecodeConductor(strings.NewReader("sandbox: {timeouts: {app_start: " + input + "}}")); err == nil {
			t.Fatal("accepted", input)
		}
	}
	if _, err := DecodeConductor(strings.NewReader("sandbox: {timeouts: {app_strat: 3s}}")); err == nil {
		t.Fatal("unknown field accepted")
	}
	c.Sandbox.Timeouts.AppStart = "-1s"
	if err := c.validateDeclarative(); err == nil || !strings.Contains(err.Error(), "sandbox.timeouts.app_start") {
		t.Fatal("programmatic override bypassed validation", err)
	}
}

func TestSandboxAppStartZeroRoundTrip(t *testing.T) {
	for _, input := range []string{"", "0", "0s", "3s"} {
		body := "sandbox: {}"
		if input != "" {
			body = "sandbox: {timeouts: {app_start: " + input + "}}"
		}
		c, err := DecodeConductor(strings.NewReader(body))
		if err != nil {
			t.Fatal(input, err)
		}
		if c.Clone().Sandbox.Timeouts.AppStart != input {
			t.Fatal("clone changed policy")
		}
		raw, err := json.Marshal(c)
		if err != nil {
			t.Fatal(err)
		}
		var round Conductor
		if err := json.Unmarshal(raw, &round); err != nil {
			t.Fatal(input, err)
		}
		if round.Sandbox.Timeouts.Runtime().AppStart != input {
			t.Fatal("policy roundtrip changed value")
		}
	}
}
