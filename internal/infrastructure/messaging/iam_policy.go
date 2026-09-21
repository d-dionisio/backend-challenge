package messaging

import (
	"encoding/json"
	"errors"
	"strings"
)

type queuePolicy struct {
	Statement []queuePolicyStatement `json:"Statement"`
}

type queuePolicyStatement struct {
	Effect    string               `json:"Effect"`
	Action    stringList           `json:"Action"`
	Principal queuePolicyPrincipal `json:"Principal"`
}

type queuePolicyPrincipal struct {
	AWS stringList `json:"AWS"`
}

type stringList []string

func (s *stringList) UnmarshalJSON(data []byte) error {
	var single string
	if json.Unmarshal(data, &single) == nil {
		*s = []string{single}
		return nil
	}
	var multiple []string
	if err := json.Unmarshal(data, &multiple); err != nil {
		return err
	}
	*s = multiple
	return nil
}

func validateQueuePolicy(raw, requiredAction string) error {
	var policy queuePolicy
	if strings.TrimSpace(raw) == "" || json.Unmarshal([]byte(raw), &policy) != nil {
		return errors.New("SQS queue requires an IAM policy")
	}
	for _, statement := range policy.Statement {
		if !strings.EqualFold(statement.Effect, "Allow") || !hasAction(statement.Action, requiredAction) {
			continue
		}
		if validPrincipals(statement.Principal.AWS) {
			return nil
		}
	}
	return errors.New("SQS queue policy does not grant the required least-privilege action to explicit principals")
}

func validateNoWildcardAdministration(raw string) error {
	var policy queuePolicy
	if strings.TrimSpace(raw) == "" || json.Unmarshal([]byte(raw), &policy) != nil {
		return errors.New("SQS queue requires an IAM policy")
	}
	for _, statement := range policy.Statement {
		if !strings.EqualFold(statement.Effect, "Allow") || !hasWildcardPrincipal(statement.Principal.AWS) {
			continue
		}
		for _, action := range statement.Action {
			if action == "*" || strings.EqualFold(action, "sqs:*") || strings.EqualFold(action, "sqs:ReceiveMessage") ||
				strings.EqualFold(action, "sqs:DeleteMessage") || strings.EqualFold(action, "sqs:ChangeMessageVisibility") ||
				strings.EqualFold(action, "sqs:SetQueueAttributes") {
				return errors.New("SQS queue policy grants unsafe wildcard administration")
			}
		}
	}
	return nil
}

func validateRedriveAllowPolicy(raw, sourceARN string) error {
	var policy struct {
		RedrivePermission string     `json:"redrivePermission"`
		SourceQueueARNs   stringList `json:"sourceQueueArns"`
	}
	if strings.TrimSpace(raw) == "" || json.Unmarshal([]byte(raw), &policy) != nil {
		return errors.New("SQS DLQ requires RedriveAllowPolicy")
	}
	if policy.RedrivePermission != "byQueue" {
		return errors.New("SQS DLQ must restrict redrive permission by source queue")
	}
	for _, arn := range policy.SourceQueueARNs {
		if arn == sourceARN {
			return nil
		}
	}
	return errors.New("SQS DLQ redrive policy does not allow the configured source queue")
}

func hasAction(actions []string, required string) bool {
	for _, action := range actions {
		if strings.EqualFold(action, required) {
			return true
		}
	}
	return false
}

func validPrincipals(principals []string) bool {
	for _, principal := range principals {
		if strings.TrimSpace(principal) == "" || principal == "*" {
			return false
		}
	}
	return len(principals) > 0
}

func hasWildcardPrincipal(principals []string) bool {
	for _, principal := range principals {
		if principal == "*" {
			return true
		}
	}
	return false
}
