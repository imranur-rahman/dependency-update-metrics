// A fake GitHub REST API with the comments of pull request 1 of any repository, for the
// end-to-end test (pullRequest.test.ts) and for the CI job that runs action.yml with target
// pull-request (fakeGitHubServer.ts). It starts with one comment of a person and accepts only
// the token "test-token".
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";

export interface StoredComment {
  id: number;
  body: string;
  html_url: string;
}

export async function fakeGitHub(port = 0): Promise<{ comments: StoredComment[]; server: Server; url: string }> {
  const comments: StoredComment[] = [{ id: 1, body: "A comment of a person", html_url: "https://github.test/c1" }];
  const server = createServer((request, response) => {
    let text = "";
    request.on("data", (chunk) => (text += chunk));
    request.on("end", () => {
      const url = new URL(request.url ?? "", "http://localhost");
      const send = (status: number, body: unknown) => {
        response.writeHead(status, { "content-type": "application/json" });
        response.end(JSON.stringify(body));
      };
      if (request.headers.authorization !== "Bearer test-token") return send(401, { message: "Bad credentials" });
      const list = /^\/repos\/[^/]+\/[^/]+\/issues\/1\/comments$/.test(url.pathname);
      if (list && request.method === "GET") {
        return send(200, url.searchParams.get("page") === "1" ? comments : []);
      }
      if (list && request.method === "POST") {
        const id = comments.length + 1;
        const comment = { id, body: JSON.parse(text).body, html_url: `https://github.test/c${id}` };
        comments.push(comment);
        return send(201, comment);
      }
      const patch = /^\/repos\/[^/]+\/[^/]+\/issues\/comments\/(\d+)$/.exec(url.pathname);
      if (patch && request.method === "PATCH") {
        const comment = comments.find((item) => item.id === Number(patch[1]));
        if (!comment) return send(404, { message: "Not Found" });
        comment.body = JSON.parse(text).body;
        return send(200, comment);
      }
      send(404, { message: "Not Found" });
    });
  });
  await new Promise<void>((resolve) => server.listen(port, "127.0.0.1", resolve));
  return { comments, server, url: `http://127.0.0.1:${(server.address() as AddressInfo).port}` };
}
