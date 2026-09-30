package slackgateway

import (
	"context"
	"testing"

	"github.com/asheshgoplani/agent-deck/internal/channelgateway"
)

func TestRowStatusReceiptUsesFixedTextAndEgressUncertaintyPath(t *testing.T) {
	store, _ := slackStore(t, channelgateway.ChannelStream)
	ctx := context.Background()
	_, err := store.Ingest(ctx, channelgateway.Inbound{
		ConversationID: testConversation, EventID: "event", MessageID: "message", ChannelID: testChannel,
		SenderID: testUser, Body: "synthetic request",
	})
	if err != nil {
		t.Fatal(err)
	}
	turn, err := store.NextTurn(ctx, testConversation)
	if err != nil {
		t.Fatal(err)
	}
	attempt, _, err := store.PrepareRowOperation(ctx, turn.ID)
	if err != nil {
		t.Fatal(err)
	}
	item, err := store.ApplyRowOperation(ctx, turn.ID, attempt.ID, channelgateway.RowOperation{
		ID: "send-id", State: channelgateway.RowIndeterminate, TerminalError: "indeterminate",
	})
	if err != nil || item == nil || item.Kind != "status" || item.Body != "" {
		t.Fatalf("status item=%+v err=%v", item, err)
	}
	sender := &fakeSender{result: PostResult{OK: true, ChannelID: testChannel, TS: "123.456"}}
	worker := &DeliveryWorker{Store: store, Sender: sender, ConversationID: testConversation, ChannelID: testChannel}
	result, err := worker.DeliverOne(ctx, item.ID)
	if err != nil || result.State != channelgateway.DeliveredDelivery || sender.text != fixedOperationFailure {
		t.Fatalf("delivery=%+v err=%v text=%q", result, err, sender.text)
	}
	if sender.text == "synthetic request" || sender.text == "indeterminate" {
		t.Fatal("status delivery exposed request or internal error")
	}
}
