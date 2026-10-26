package main

import "testing"

// Step names go into job ids (run/step/n) and device ids into NATS subjects
// (lab.cmd.<device>), so both are refused when they would break either.
func TestValidateWorkflowRefusesUnsafeNames(t *testing.T) {
	cases := []struct {
		name string
		step StepTemplate
		ok   bool
	}{
		{"plain", StepTemplate{Name: "fill_plate-2", DeviceID: "liquid-handler-1"}, true},
		{"slash in step", StepTemplate{Name: "fill/plate", DeviceID: "lh"}, false},
		{"space in step", StepTemplate{Name: "fill plate", DeviceID: "lh"}, false},
		{"dot in device", StepTemplate{Name: "fill", DeviceID: "lh.1"}, false},
		{"wildcard device", StepTemplate{Name: "fill", DeviceID: ">"}, false},
		{"star in device", StepTemplate{Name: "fill", DeviceID: "lh*"}, false},
	}
	for _, c := range cases {
		err := validateWorkflow(workflowTemplate{Name: "w", Steps: []StepTemplate{c.step}})
		if (err == nil) != c.ok {
			t.Errorf("%s: err = %v, want ok = %v", c.name, err, c.ok)
		}
	}
}

func TestShippedWorkflowsLoad(t *testing.T) {
	if _, err := LoadWorkflows("../../config/workflows.yaml"); err != nil {
		t.Fatal(err)
	}
}
