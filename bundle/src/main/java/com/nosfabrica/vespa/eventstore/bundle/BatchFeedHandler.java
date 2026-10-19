/*
 * Copyright (c) 2026 NosFabrica
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the
 * Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN
 * AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package com.nosfabrica.vespa.eventstore.bundle;

import com.fasterxml.jackson.core.JsonFactory;
import com.yahoo.container.jdisc.HttpRequest;
import com.yahoo.container.jdisc.HttpResponse;
import com.yahoo.container.jdisc.ThreadedHttpRequestHandler;
import com.yahoo.document.DocumentOperation;
import com.yahoo.document.DocumentPut;
import com.yahoo.document.DocumentRemove;
import com.yahoo.document.DocumentTypeManager;
import com.yahoo.document.json.JsonReader;
import com.yahoo.documentapi.AsyncParameters;
import com.yahoo.documentapi.AsyncSession;
import com.yahoo.documentapi.DocumentAccess;
import com.yahoo.documentapi.DocumentOperationParameters;
import com.yahoo.documentapi.Response;
import com.yahoo.documentapi.Result;
import com.yahoo.jdisc.Metric;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * MANY document operations per HTTP request. {@code /document/v1} takes one, and on a warm engine
 * most of what one costs the container is the request around the document rather than the
 * document (benchmark/README.md, "The fixed cost of a put, on a warm engine"): batching 100 per
 * request cut Vespa's CPU per live-relay event by about 31% ("More than one document per
 * request").
 *
 * <p>The body is a standard Vespa JSON feed: an array of operations, each exactly what
 * {@code /document/v1} would take for that document plus its id ({@code {"put": "id:…",
 * "fields": {…}}}, or {@code {"remove": "id:…"}}). Every operation is dispatched through the
 * container's own document API, on the default route, so it passes the same indexing chain and
 * the same routing selection a {@code /document/v1} put does: the stored document is the same.
 *
 * <p>The response is written once EVERY operation has been answered, so an HTTP 200 carries the
 * same promise a {@code /document/v1} 200 does, per operation: acked means stored and visible to
 * search. It lists the operations that did not succeed, by their position in the request; an
 * operation the routing selection dropped (an expired event) counts as a success, as it does on
 * {@code /document/v1}. A body that does not parse fails the whole request with a 400 before
 * anything is sent.
 */
public class BatchFeedHandler extends ThreadedHttpRequestHandler {

    /** How long one request may wait for its operations, when it names no {@code timeout} (seconds). */
    static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);

    private final DocumentTypeManager types;
    private final AsyncSession session;
    private final JsonFactory json = new JsonFactory();

    /**
     * Signalled once per answered operation: what a sender waiting on a full session window
     * sleeps on. Re-trying on a timer instead burned the handler's threads (measured: 155 µs of
     * handler-thread CPU per put, against 77 for the container's own batched endpoint, which
     * blocks).
     */
    private final Object window = new Object();

    public BatchFeedHandler(Executor executor, Metric metric, DocumentAccess access) {
        super(executor, metric);
        this.types = access.getDocumentTypeManager();
        this.session = access.createAsyncSession(new AsyncParameters());
    }

    @Override
    public HttpResponse handle(HttpRequest request) {
        if (request.getMethod() != com.yahoo.jdisc.http.HttpRequest.Method.POST)
            return new Json(405, "{\"error\":\"POST a JSON array of operations\"}");
        List<DocumentOperation> operations = new ArrayList<>();
        try {
            JsonReader reader = new JsonReader(types, request.getData(), json);
            for (DocumentOperation operation; (operation = reader.next()) != null; ) operations.add(operation);
        } catch (RuntimeException e) {
            return new Json(400, "{\"error\":" + quote(String.valueOf(e.getMessage())) + "}");
        }
        Duration timeout = timeout(request);
        Instant deadline = Instant.now().plus(timeout);
        int n = operations.size();
        String[] outcome = new String[n];
        String[] message = new String[n];
        CountDownLatch answered = new CountDownLatch(n);
        for (int i = 0; i < n; i++) {
            int index = i;
            DocumentOperationParameters parameters = DocumentOperationParameters.parameters()
                    .withDeadline(deadline)
                    .withResponseHandler(response -> {
                        if (!succeeded(response)) {
                            outcome[index] = response.outcome().name();
                            message[index] = response.getTextMessage();
                        }
                        answered.countDown();
                        synchronized (window) { window.notify(); }
                    });
            String refused = send(operations.get(i), parameters, deadline);
            if (refused != null) {
                outcome[i] = "REJECTED";
                message[i] = refused;
                answered.countDown();
            }
        }
        try {
            // The operations carry the same deadline; the slack covers a reply already on its way.
            if (!answered.await(Duration.between(Instant.now(), deadline).toMillis() + 5_000, TimeUnit.MILLISECONDS))
                return new Json(504, "{\"error\":\"operations still unanswered at the deadline\"}");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new Json(503, "{\"error\":\"interrupted\"}");
        }
        StringBuilder out = new StringBuilder(64).append("{\"count\":").append(n).append(",\"failed\":[");
        boolean first = true;
        for (int i = 0; i < n; i++) {
            if (outcome[i] == null) continue;
            if (!first) out.append(',');
            first = false;
            out.append("{\"index\":").append(i)
               .append(",\"outcome\":").append(quote(outcome[i]))
               .append(",\"message\":").append(quote(String.valueOf(message[i]))).append('}');
        }
        return new Json(200, out.append("]}").toString());
    }

    /**
     * Hands one operation to the session, waiting while the session's window is full (it answers
     * {@code TRANSIENT_ERROR} then, and the caller is expected to retry). Null when it was sent,
     * else why not.
     */
    private String send(DocumentOperation operation, DocumentOperationParameters parameters, Instant deadline) {
        while (true) {
            Result result;
            if (operation instanceof DocumentPut put) result = session.put(put, parameters);
            else if (operation instanceof DocumentRemove remove) result = session.remove(remove, parameters);
            else return "only put and remove are supported, not " + operation.getClass().getSimpleName();
            switch (result.type()) {
                case SUCCESS:
                    return null;
                case TRANSIENT_ERROR:
                    if (Instant.now().isAfter(deadline)) return "the session stayed full until the deadline: " + result.error();
                    // Full: sleep until an answer frees a slot. The timeout only covers a slot
                    // freed by something this handler does not hear about.
                    try {
                        synchronized (window) { window.wait(5); }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return "interrupted";
                    }
                    break;
                default:
                    return String.valueOf(result.error());
            }
        }
    }

    /** Stored, or deliberately not stored: the routing selection drops an expired event, as it would on /document/v1. */
    private static boolean succeeded(Response response) {
        return response.isSuccess() || response.outcome() == Response.Outcome.IGNORED;
    }

    private static Duration timeout(HttpRequest request) {
        String value = request.getProperty("timeout");
        if (value == null) return DEFAULT_TIMEOUT;
        try {
            return Duration.ofMillis((long) (Double.parseDouble(value.replace("s", "")) * 1000));
        } catch (NumberFormatException e) {
            return DEFAULT_TIMEOUT;
        }
    }

    static String quote(String s) {
        StringBuilder out = new StringBuilder(s.length() + 2).append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"': out.append("\\\""); break;
                case '\\': out.append("\\\\"); break;
                case '\n': out.append("\\n"); break;
                case '\r': out.append("\\r"); break;
                case '\t': out.append("\\t"); break;
                default:
                    if (c < 0x20) out.append(String.format("\\u%04x", (int) c));
                    else out.append(c);
            }
        }
        return out.append('"').toString();
    }

    @Override
    protected void destroy() {
        session.destroy();
        super.destroy();
    }

    private static final class Json extends HttpResponse {
        private final byte[] body;

        Json(int status, String body) {
            super(status);
            this.body = body.getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public String getContentType() {
            return "application/json";
        }

        @Override
        public void render(OutputStream out) throws IOException {
            out.write(body);
        }
    }
}
