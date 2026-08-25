{{- define "insight-platform-capability-native-worker.name" -}}
insight-platform-capability-native-worker
{{- end }}
{{- define "insight-platform-capability-native-worker.labels" -}}
app.kubernetes.io/name: {{ include "insight-platform-capability-native-worker.name" . }}
app.kubernetes.io/component: capability-native-worker
app.kubernetes.io/part-of: insight-platform
{{- end }}
{{- define "insight-platform-capability-native-worker.image" -}}
{{ printf "%s@%s" .Values.image.repository .Values.image.digest }}
{{- end }}
