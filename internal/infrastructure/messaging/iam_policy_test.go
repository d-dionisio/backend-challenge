package messaging

import "testing"

func TestValidateQueuePolicy(t *testing.T) {
	policy := `{"Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::123456789012:role/provider-a"},"Action":"sqs:SendMessage"}]}`
	if err := validateQueuePolicy(policy, "sqs:SendMessage"); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []string{
		``,
		`{"Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"sqs:SendMessage"}]}`,
		`{"Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::123456789012:role/provider-a"},"Action":"sqs:ReceiveMessage"}]}`,
	} {
		if err := validateQueuePolicy(invalid, "sqs:SendMessage"); err == nil {
			t.Fatal("invalid queue policy accepted")
		}
	}
}

func TestValidateNoWildcardAdministration(t *testing.T) {
	if err := validateNoWildcardAdministration(`{"Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"sqs:SendMessage"}]}`); err != nil {
		t.Fatal(err)
	}
	if err := validateNoWildcardAdministration(`{"Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":["sqs:ReceiveMessage"]}]}`); err == nil {
		t.Fatal("wildcard consumer policy accepted")
	}
}

func TestValidateRedriveAllowPolicy(t *testing.T) {
	source := "arn:aws:sqs:us-east-1:123456789012:wager-transactions.fifo"
	policy := `{"redrivePermission":"byQueue","sourceQueueArns":["arn:aws:sqs:us-east-1:123456789012:wager-transactions.fifo"]}`
	if err := validateRedriveAllowPolicy(policy, source); err != nil {
		t.Fatal(err)
	}
	if err := validateRedriveAllowPolicy(`{"redrivePermission":"allowAll"}`, source); err == nil {
		t.Fatal("unsafe redrive policy accepted")
	}
}
