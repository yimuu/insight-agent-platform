{{- define "insight-platform-mcp-cleanup-worker.name" -}}
{{- printf "%s-mcp-cleanup-worker" .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "insight-platform-mcp-cleanup-worker.image" -}}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest -}}
{{- end -}}

{{- define "insight-platform-mcp-cleanup-worker.labels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: mcp-cleanup-worker
{{- end -}}
