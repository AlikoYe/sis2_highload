package main

import (
	"log/slog"

	notificationsv1 "sis2_highload/gen/notifications/v1"
)

func main() {
	slog.Info("server not implemented yet", "service", notificationsv1.NotificationService_ServiceDesc.ServiceName)
}
