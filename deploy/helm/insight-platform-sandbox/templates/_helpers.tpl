{{- define "insight-platform-sandbox.name" -}}
insight-platform-sandbox
{{- end }}

{{- define "insight-platform-sandbox.labels" -}}
app.kubernetes.io/name: {{ include "insight-platform-sandbox.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- end }}

{{- define "insight-platform-sandbox.image" -}}
{{- printf "%s@%s" .repository .digest -}}
{{- end }}

{{/* Names used by the source-pinned upstream CRD templates. */}}
{{- define "opensandbox.labels" -}}
app.kubernetes.io/name: opensandbox-controller
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: "0.2.0"
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: opensandbox-controller-0.2.1
insight.platform/upstream-commit: {{ .Values.global.sourceCommit | quote }}
{{- end }}

{{- define "insight-platform-sandbox.runtimeContract" -}}
{{- dict
  "schema_version" 1
  "provider" "open_sandbox_kubernetes"
  "opensandbox_server_release_digest" .Values.images.server.digest
  "lifecycle_schema_digest" .Values.runtimeContract.lifecycleSchemaDigest
  "batchsandbox_crd_digest" .Values.runtimeContract.batchSandboxCrdDigest
  "batchsandbox_controller_digest" .Values.images.controller.digest
  "kubernetes_provider_template_digest" .Values.runtimeContract.kubernetesProviderTemplateDigest
  "runner_protocol_digest" .Values.runtimeContract.runnerProtocolDigest
  "container_runtime_digest" .Values.runtimeContract.containerRuntimeDigest
  "network_policy_digest" .Values.runtimeContract.networkPolicyDigest
  | toJson -}}
{{- end }}

{{- define "insight-platform-sandbox.runtimeContractDigest" -}}
sha256:{{ include "insight-platform-sandbox.runtimeContract" . | sha256sum }}
{{- end }}
