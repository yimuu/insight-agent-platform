{{- define "insight-platform-callback-api.name" -}}
{{- printf "%s-callback-api" .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "insight-platform-callback-api.image" -}}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest -}}
{{- end -}}

{{- define "insight-platform-callback-api.labels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: mcp-callback-api
{{- end -}}
