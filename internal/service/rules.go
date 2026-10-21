package service

import (
	"slices"

	"sis2_highload/internal/domain"
)

var rules = map[string][]domain.Channel{
	"order.shipped":   {domain.ChannelPush, domain.ChannelEmail},
	"payment.failed":  {domain.ChannelPush, domain.ChannelEmail},
	"auth.login_code": {domain.ChannelPush},
}

// Channels returns the channels an event type fans out to.
func Channels(eventType string) ([]domain.Channel, bool) {
	ch, ok := rules[eventType]
	return slices.Clone(ch), ok
}
