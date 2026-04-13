{{/*
Shared pod spec used by both Deployment and StatefulSet.

Volumes:
  - "config"  — rendered depotd.toml at /etc/depot (Secret)
  - "data"    — mounted at /data when on-disk storage is needed (redb KV
                 and/or file blob store). In StatefulSet mode the volume is
                 provided automatically by volumeClaimTemplates; in
                 Deployment mode we reference a standalone PVC.
*/}}
{{- define "depot.needsDataVolume" -}}
{{- if or (eq .Values.kvStore.type "redb") (and (eq .Values.blobStore.type "file") .Values.blobStore.file.persistence.enabled) -}}
true
{{- else -}}
false
{{- end -}}
{{- end -}}

{{- define "depot.podSpec" -}}
serviceAccountName: {{ include "depot.serviceAccountName" . }}
{{- with .Values.image.pullSecrets }}
imagePullSecrets:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with .Values.podSecurityContext }}
securityContext:
  {{- toYaml . | nindent 2 }}
{{- end }}
containers:
  - name: depot
    image: {{ include "depot.image" . }}
    imagePullPolicy: {{ .Values.image.pullPolicy }}
    args:
      - "-c"
      - "/etc/depot/depotd.toml"
    ports:
      - name: http
        containerPort: 8080
        protocol: TCP
      {{- if .Values.config.metricsListen }}
      - name: metrics
        containerPort: 9090
        protocol: TCP
      {{- end }}
    {{- if .Values.aws.enabled }}
    env:
      - name: AWS_ACCESS_KEY_ID
        valueFrom:
          secretKeyRef:
            name: {{ include "depot.awsSecretEffective" . }}
            key: access-key-id
      - name: AWS_SECRET_ACCESS_KEY
        valueFrom:
          secretKeyRef:
            name: {{ include "depot.awsSecretEffective" . }}
            key: secret-access-key
      {{- if .Values.aws.region }}
      - name: AWS_REGION
        value: {{ .Values.aws.region | quote }}
      {{- end }}
    {{- end }}
    {{- if .Values.probes.liveness.enabled }}
    livenessProbe:
      httpGet:
        path: /api/v1/health
        port: http
      initialDelaySeconds: {{ .Values.probes.liveness.initialDelaySeconds }}
      periodSeconds: {{ .Values.probes.liveness.periodSeconds }}
      timeoutSeconds: {{ .Values.probes.liveness.timeoutSeconds }}
      failureThreshold: {{ .Values.probes.liveness.failureThreshold }}
    {{- end }}
    {{- if .Values.probes.readiness.enabled }}
    readinessProbe:
      httpGet:
        path: /api/v1/health
        port: http
      initialDelaySeconds: {{ .Values.probes.readiness.initialDelaySeconds }}
      periodSeconds: {{ .Values.probes.readiness.periodSeconds }}
      timeoutSeconds: {{ .Values.probes.readiness.timeoutSeconds }}
      failureThreshold: {{ .Values.probes.readiness.failureThreshold }}
    {{- end }}
    {{- with .Values.resources }}
    resources:
      {{- toYaml . | nindent 6 }}
    {{- end }}
    {{- with .Values.securityContext }}
    securityContext:
      {{- toYaml . | nindent 6 }}
    {{- end }}
    volumeMounts:
      - name: config
        mountPath: /etc/depot
        readOnly: true
      {{- if eq (include "depot.needsDataVolume" .) "true" }}
      - name: data
        mountPath: /data
      {{- end }}
volumes:
  - name: config
    secret:
      secretName: {{ include "depot.configSecretName" . }}
  {{- if and (eq (include "depot.needsDataVolume" .) "true") (ne (include "depot.isStateful" .) "true") }}
  - name: data
    persistentVolumeClaim:
      claimName: {{ include "depot.fullname" . }}-data
  {{- end }}
{{- with .Values.nodeSelector }}
nodeSelector:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with .Values.tolerations }}
tolerations:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with .Values.affinity }}
affinity:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- end -}}
