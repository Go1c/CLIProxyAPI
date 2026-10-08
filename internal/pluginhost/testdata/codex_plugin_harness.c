#include "codex-http2-keepalive.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int host_call(void *ctx, const char *method, const uint8_t *request, size_t request_len, cliproxy_buffer *response) {
	(void)ctx;
	(void)method;
	(void)request;
	(void)request_len;
	const char *payload = "{\"ok\":true,\"result\":{\"canceled\":false}}";
	response->len = strlen(payload);
	response->ptr = malloc(response->len);
	if (response->ptr == NULL) {
		response->len = 0;
		return 1;
	}
	memcpy(response->ptr, payload, response->len);
	return 0;
}

static void host_free(void *ptr, size_t len) {
	(void)len;
	free(ptr);
}

static int call_plugin(cliproxy_plugin_api *plugin, const char *label, const char *method, const char *request) {
	cliproxy_buffer response = {0};
	int rc = plugin->call((char *)method, (uint8_t *)request, strlen(request), &response);
	printf("%s:", label);
	if (response.ptr != NULL && response.len > 0) {
		fwrite(response.ptr, 1, response.len, stdout);
		plugin->free_buffer(response.ptr, response.len);
	}
	putchar('\n');
	return rc;
}

int main(void) {
	cliproxy_host_api host = {
		.abi_version = 1,
		.host_ctx = NULL,
		.call = host_call,
		.free_buffer = host_free,
	};
	cliproxy_plugin_api plugin = {0};
	if (cliproxy_plugin_init(&host, &plugin) != 0) {
		return 10;
	}

	const char *registration = "{\"config_yaml\":\"ZW5hYmxlZDogdHJ1ZQpiYXNlX3VybDogaHR0cHM6Ly8xMjcuMC4wLjE6MS9yZXNwb25zZXMK\",\"schema_version\":1}";
	if (call_plugin(&plugin, "register", "plugin.register", registration) != 0) {
		return 11;
	}
	if (call_plugin(&plugin, "identifier", "executor.identifier", "{}") != 0) {
		return 12;
	}

	const char *execution = "{\"AuthID\":\"codex-oauth\",\"AuthProvider\":\"codex\",\"Model\":\"gpt-5.4\",\"Format\":\"codex\",\"Payload\":\"eyJtb2RlbCI6ImdwdC01LjQiLCJpbnB1dCI6W119\",\"StorageJSON\":\"eyJhY2Nlc3NfdG9rZW4iOiJvYXV0aC10b2tlbiIsImJhc2VfdXJsIjoiaHR0cHM6Ly8xMjcuMC4wLjE6MS9yZXNwb25zZXMifQ==\",\"AuthMetadata\":{\"access_token\":\"oauth-token\"},\"AuthAttributes\":{\"base_url\":\"https://127.0.0.1:1/responses\"},\"host_callback_id\":\"1\"}";
	if (call_plugin(&plugin, "execute", "executor.execute", execution) != 0) {
		return 13;
	}
	plugin.shutdown();
	return 0;
}
