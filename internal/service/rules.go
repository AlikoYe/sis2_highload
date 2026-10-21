package service

import "sis2_highload/internal/domain"

var rules = map[string][]domain.Channel{
	"order.shipped":   {domain.ChannelPush, domain.ChannelEmail},
	"payment.failed":  {domain.ChannelPush, domain.ChannelEmail},
	"auth.login_code": {domain.ChannelPush},
}
