{{- define "insight-platform-history-maintenance.name" -}}
{{- printf "%s-history-maintenance" .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "insight-platform-history-maintenance.image" -}}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest -}}
{{- end -}}

{{- define "insight-platform-history-maintenance.labels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: history-maintenance
{{- end -}}
