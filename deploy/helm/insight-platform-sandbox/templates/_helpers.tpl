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

{{- define "insight-platform-sandbox.microVmExecutorName" -}}
{{- printf "%s-executor-microvm" (include "insight-platform-sandbox.name" .) | trunc 63 | trimSuffix "-" -}}
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
  "database_max_connections" (int .Values.controller.database.maxConnections)
  "artifact_provider_catalog" .Values.controller.artifactProviderCatalog
  "artifact_broker" (dict
    "maximum_in_flight" (int .Values.controller.artifactBroker.maximum_in_flight)
    "maximum_read_bytes" (int .Values.controller.artifactBroker.maximum_read_bytes)
    "operation_timeout_milliseconds" (int .Values.controller.artifactBroker.operation_timeout_milliseconds))
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
  "receipt_ttl_seconds" (int .Values.executor.receiptTtlSeconds)
  "claim_scan_milliseconds" (int .Values.executor.claimScanMilliseconds)
  "claim_failure_backoff_milliseconds" (int .Values.executor.claimFailureBackoffMilliseconds)
  "drain_grace_milliseconds" (int .Values.executor.drainGraceMilliseconds)
  "control_request_timeout_milliseconds" (int .Values.executor.controlRequestTimeoutMilliseconds)
  "connect_timeout_milliseconds" (int .Values.executor.connectTimeoutMilliseconds)
  "request_timeout_milliseconds" (int .Values.executor.requestTimeoutMilliseconds)
  | toJson -}}
{{- end }}

{{- define "insight-platform-sandbox.microVmExecutorConfig" -}}
{{- dict
  "schema_version" 1
  "worker_manifest" .Values.microVmExecutor.workerManifest
  "backend" (dict
    "kind" "micro_vm"
    "provider_socket_path" .Values.microVmExecutor.provider.socketPath
    "provider_tls_server_name" .Values.microVmExecutor.provider.tlsServerName)
  "backend_contract_digest" .Values.microVmExecutor.backendContractDigest
  "authority_endpoint" (include "insight-platform-sandbox.controllerEndpoint" .)
  "authority_tls_server_name" (printf "%s.%s.svc" (include "insight-platform-sandbox.controllerName" .) .Values.namespaces.controller)
  "process_registration_attestor_socket_path" "/run/insight-sandbox-attestor/registration.sock"
  "process_registration_attestor_tls_server_name" .Values.controller.attestor.tlsServerName
  "process_registration_attestor_identity_digest" .Values.controller.attestor.identityDigest
  "nats_endpoint" .Values.microVmExecutor.natsEndpoint
  "receipt_ttl_seconds" (int .Values.microVmExecutor.receiptTtlSeconds)
  "claim_scan_milliseconds" (int .Values.microVmExecutor.claimScanMilliseconds)
  "claim_failure_backoff_milliseconds" (int .Values.microVmExecutor.claimFailureBackoffMilliseconds)
  "drain_grace_milliseconds" (int .Values.microVmExecutor.drainGraceMilliseconds)
  "control_request_timeout_milliseconds" (int .Values.microVmExecutor.controlRequestTimeoutMilliseconds)
  "connect_timeout_milliseconds" (int .Values.microVmExecutor.connectTimeoutMilliseconds)
  "request_timeout_milliseconds" (int .Values.microVmExecutor.requestTimeoutMilliseconds)
  | toJson -}}
{{- end }}

{{- define "insight-platform-sandbox.microVmProviderConfig" -}}
{{- dict
  "schema_version" 1
  "provider_socket_path" .Values.microVmExecutor.provider.socketPath
  "provider_tls_server_name" .Values.microVmExecutor.provider.tlsServerName
  "controller_broker_endpoint" (include "insight-platform-sandbox.controllerEndpoint" .)
  "controller_broker_tls_server_name" (printf "%s.%s.svc" (include "insight-platform-sandbox.controllerName" .) .Values.namespaces.controller)
  "worker_manifest_digest" (printf "sha256:%s" (toJson .Values.microVmExecutor.workerManifest | sha256sum))
  "backend_contract_digest" .Values.microVmExecutor.backendContractDigest
  "installation" .Values.microVmExecutor.provider.installation
  "runtimes" .Values.microVmExecutor.provider.runtimes
  "state_directory" .Values.microVmExecutor.provider.stateDirectory
  "ephemeral_uid_base" (int .Values.microVmExecutor.provider.ephemeralUidBase)
  "ephemeral_gid_base" (int .Values.microVmExecutor.provider.ephemeralGidBase)
  "ephemeral_identity_count" (int .Values.microVmExecutor.provider.ephemeralIdentityCount)
  "maximum_instances" (int .Values.microVmExecutor.provider.maximumInstances)
  "maximum_tombstones" (int .Values.microVmExecutor.provider.maximumTombstones)
  "tombstone_retention_seconds" (int .Values.microVmExecutor.provider.tombstoneRetentionSeconds)
  "maximum_lifecycle_entries" (int .Values.microVmExecutor.provider.maximumLifecycleEntries)
  "api_timeout_milliseconds" (int .Values.microVmExecutor.provider.apiTimeoutMilliseconds)
  "guest_channel_timeout_milliseconds" (int .Values.microVmExecutor.provider.guestChannelTimeoutMilliseconds)
  "socket_poll_milliseconds" (int .Values.microVmExecutor.provider.socketPollMilliseconds)
  "process_termination_timeout_milliseconds" (int .Values.microVmExecutor.provider.processTerminationTimeoutMilliseconds)
  "connect_timeout_milliseconds" (int .Values.microVmExecutor.provider.connectTimeoutMilliseconds)
  "request_timeout_milliseconds" (int .Values.microVmExecutor.provider.requestTimeoutMilliseconds)
  "tls_handshake_timeout_milliseconds" (int .Values.microVmExecutor.provider.tlsHandshakeTimeoutMilliseconds)
  "shutdown_grace_milliseconds" (int .Values.microVmExecutor.provider.shutdownGraceMilliseconds)
  | toJson -}}
{{- end }}
