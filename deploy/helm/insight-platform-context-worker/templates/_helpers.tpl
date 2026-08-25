{{- define "insight-platform-context-worker.name" -}}
insight-platform-context-worker
{{- end }}

{{- define "insight-platform-context-worker.labels" -}}
app.kubernetes.io/name: {{ include "insight-platform-context-worker.name" . }}
app.kubernetes.io/component: context-worker
app.kubernetes.io/part-of: insight-platform
{{- end }}

{{- define "insight-platform-context-worker.image" -}}
{{ printf "%s@%s" .Values.image.repository .Values.image.digest }}
{{- end }}

{{- define "insight-platform-context-worker.subscriptionName" -}}
insight-platform-subscription-context-worker
{{- end }}

{{- define "insight-platform-context-worker.subscriptionLabels" -}}
app.kubernetes.io/name: {{ include "insight-platform-context-worker.subscriptionName" . }}
app.kubernetes.io/component: context-subscription-worker
app.kubernetes.io/part-of: insight-platform
{{- end }}
