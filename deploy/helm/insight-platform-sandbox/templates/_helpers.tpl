{{- define "insight-platform-sandbox.name" -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end }}
{{- define "insight-platform-sandbox.labels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "insight-platform-sandbox.controllerName" -}}
{{- printf "%s-controller" (include "insight-platform-sandbox.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{- define "insight-platform-sandbox.attestorName" -}}
{{- printf "%s-attestor" (include "insight-platform-sandbox.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{- define "insight-platform-sandbox.executorName" -}}
{{- printf "%s-executor-wasi" (include "insight-platform-sandbox.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{- define "insight-platform-sandbox.gvisorName" -}}
{{- printf "%s-executor-gvisor" (include "insight-platform-sandbox.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{- define "insight-platform-sandbox.image" -}}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest -}}
{{- end }}

{{- define "insight-platform-sandbox.controllerEndpoint" -}}
{{- printf "https://%s.%s.svc:%d" (include "insight-platform-sandbox.controllerName" .) .Values.namespaces.controller (int .Values.controller.port) -}}
{{- end }}

{{- define "insight-platform-sandbox.controllerConfig" -}}
{{- dict
  "schema_version" 1
  "listen_address" (printf "0.0.0.0:%d" (int .Values.controller.port))
  "observability_listen_address" (printf "0.0.0.0:%d" (int .Values.controller.observabilityPort))
  "database_max_connections" (int .Values.controller.database.maxConnections)
  "artifact_broker" (dict
    "endpoint" .Values.controller.artifactBroker.endpoint
    "tls_server_name" .Values.controller.artifactBroker.tlsServerName
    "maximum_request_bytes" (int .Values.controller.artifactBroker.maximumRequestBytes)
    "maximum_chunk_bytes" (int .Values.controller.artifactBroker.maximumChunkBytes)
    "maximum_in_flight_responses" (int .Values.controller.artifactBroker.maximumInFlightResponses))
  "process_isolation_attestor" (dict
    "tls_server_name" .Values.controller.attestor.tlsServerName
    "attestor_identity_digest" .Values.controller.attestor.identityDigest
    "maximum_cached_routes" (int .Values.controller.attestor.maximumCachedRoutes)
    "controller_port" (int .Values.attestor.controllerPort)
    "allowed_node_cidrs" .Values.controller.attestor.allowedNodeCidrs)
  "connect_timeout_milliseconds" (int .Values.controller.connectTimeoutMilliseconds)
  "request_timeout_milliseconds" (int .Values.controller.requestTimeoutMilliseconds)
  "shutdown_grace_milliseconds" (int .Values.controller.shutdownGraceMilliseconds)
  | toJson -}}
{{- end }}

{{- define "insight-platform-sandbox.attestorConfig" -}}
{{- dict
  "schema_version" 1
  "registration_socket_path" "/run/insight-sandbox-attestor/registration.sock"
  "controller_listen_address" (printf "0.0.0.0:%d" (int .Values.attestor.controllerPort))
  "proc_root" "/host/proc"
  "node_uid_authority_path" "/host/node-uid"
  "registry_path" "/var/lib/insight-sandbox-attestor/registrations.json"
  "maximum_registrations" (int .Values.attestor.maximumRegistrations)
  "absent_retention_seconds" (int .Values.attestor.absentRetentionSeconds)
  "attestor_identity_digest" .Values.controller.attestor.identityDigest
  "tls_handshake_timeout_milliseconds" (int .Values.attestor.tlsHandshakeTimeoutMilliseconds)
  "shutdown_grace_milliseconds" (int .Values.attestor.shutdownGraceMilliseconds)
  | toJson -}}
{{- end }}

{{- define "insight-platform-sandbox.executorConfig" -}}
{{- dict
  "schema_version" 1
  "worker_manifest" .Values.executor.workerManifest
  "backend" (dict
    "kind" "wasi"
    "runtime_version" .Values.executor.runtimeVersion)
  "backend_contract_digest" .Values.executor.backendContractDigest
  "authority_endpoint" (include "insight-platform-sandbox.controllerEndpoint" .)
  "authority_tls_server_name" (printf "%s.%s.svc" (include "insight-platform-sandbox.controllerName" .) .Values.namespaces.controller)
  "process_registration_attestor_socket_path" "/run/insight-sandbox-attestor/registration.sock"
  "process_registration_attestor_tls_server_name" .Values.controller.attestor.tlsServerName
  "process_registration_attestor_identity_digest" .Values.controller.attestor.identityDigest
  "nats_endpoint" .Values.executor.natsEndpoint
  "observability_listen_address" (printf "0.0.0.0:%d" (int .Values.executor.observabilityPort))
  "receipt_ttl_seconds" (int .Values.executor.receiptTtlSeconds)
  "claim_scan_milliseconds" (int .Values.executor.claimScanMilliseconds)
  "claim_failure_backoff_milliseconds" (int .Values.executor.claimFailureBackoffMilliseconds)
  "drain_grace_milliseconds" (int .Values.executor.drainGraceMilliseconds)
  "control_request_timeout_milliseconds" (int .Values.executor.controlRequestTimeoutMilliseconds)
  "connect_timeout_milliseconds" (int .Values.executor.connectTimeoutMilliseconds)
  "request_timeout_milliseconds" (int .Values.executor.requestTimeoutMilliseconds)
  | toJson -}}
{{- end }}

{{- define "insight-platform-sandbox.gvisorAttestorConfig" -}}
{{- dict
  "schema_version" 1
  "registration_socket_path" "/run/insight-sandbox-attestor/registration.sock"
  "controller_listen_address" (printf "0.0.0.0:%d" (int .Values.attestor.controllerPort))
  "proc_root" "/proc"
  "node_uid_authority_path" "/etc/insight/podinfo/uid"
  "registry_path" "/var/lib/insight-sandbox-attestor/registrations.json"
  "maximum_registrations" (int .Values.gvisor.attestor.maximumRegistrations)
  "absent_retention_seconds" (int .Values.gvisor.attestor.absentRetentionSeconds)
  "attestor_identity_digest" .Values.controller.attestor.identityDigest
  "tls_handshake_timeout_milliseconds" (int .Values.gvisor.attestor.tlsHandshakeTimeoutMilliseconds)
  "shutdown_grace_milliseconds" (int .Values.gvisor.attestor.shutdownGraceMilliseconds)
  | toJson -}}
{{- end }}

{{- define "insight-platform-sandbox.gvisorConfig" -}}
{{- dict
  "schema_version" 1
  "worker_manifest" .Values.gvisor.workerManifest
  "backend" (dict
    "kind" "gvisor"
    "kubernetes" (dict
      "namespace" .Values.namespaces.guest
      "runtime_class_name" .Values.gvisor.runtimeClassName
      "guest_service_account_name" .Values.gvisor.guestServiceAccount
      "guest_image_repository" .Values.gvisor.guestImageRepository
      "guest_command" .Values.gvisor.guestCommand
      "bootstrap_endpoint" .Values.gvisor.bootstrapEndpoint
      "bootstrap_ca_path" .Values.gvisor.bootstrapCaPath
      "bootstrap_token_audience" .Values.gvisor.bootstrapTokenAudience
      "bootstrap_token_expiration_seconds" (int .Values.gvisor.bootstrapTokenExpirationSeconds)
      "observation_poll_milliseconds" (int .Values.gvisor.observationPollMilliseconds)))
  "backend_contract_digest" .Values.gvisor.backendContractDigest
  "authority_endpoint" (include "insight-platform-sandbox.controllerEndpoint" .)
  "authority_tls_server_name" (printf "%s.%s.svc" (include "insight-platform-sandbox.controllerName" .) .Values.namespaces.controller)
  "process_registration_attestor_socket_path" "/run/insight-sandbox-attestor/registration.sock"
  "process_registration_attestor_tls_server_name" .Values.controller.attestor.tlsServerName
  "process_registration_attestor_identity_digest" .Values.controller.attestor.identityDigest
  "nats_endpoint" .Values.executor.natsEndpoint
  "observability_listen_address" (printf "0.0.0.0:%d" (int .Values.gvisor.observabilityPort))
  "receipt_ttl_seconds" (int .Values.gvisor.receiptTtlSeconds)
  "claim_scan_milliseconds" (int .Values.gvisor.claimScanMilliseconds)
  "claim_failure_backoff_milliseconds" (int .Values.gvisor.claimFailureBackoffMilliseconds)
  "drain_grace_milliseconds" (int .Values.gvisor.drainGraceMilliseconds)
  "control_request_timeout_milliseconds" (int .Values.gvisor.controlRequestTimeoutMilliseconds)
  "connect_timeout_milliseconds" (int .Values.gvisor.connectTimeoutMilliseconds)
  "request_timeout_milliseconds" (int .Values.gvisor.requestTimeoutMilliseconds)
  | toJson -}}
{{- end }}
