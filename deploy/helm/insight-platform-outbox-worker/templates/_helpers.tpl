{{- define "insight-platform-outbox-worker.name" -}}
{{- printf "%s-outbox-worker" .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "insight-platform-outbox-worker.image" -}}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest -}}
{{- end -}}

{{- define "insight-platform-outbox-worker.labels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: outbox-worker
{{- end -}}
