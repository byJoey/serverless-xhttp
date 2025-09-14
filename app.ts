import { serve } from "https://deno.land/std/http/server.ts";

const UUID: string = Deno.env.get("UUID") || "b70a3a2f-5eae-4311-ad37-29a30536e59b";
const SUB_PATH: string = Deno.env.get("SUB_PATH") || "sub";  //获取订阅路径
const XPATH: string = Deno.env.get("XPATH") || "xhttp";      // 节点路径
const DOMAIN: string = Deno.env.get("DOMAIN") || "";         // deno分配的域名必填，不带https://前缀，例如：xxxx.deno.dev      
const NAME: string = Deno.env.get("NAME") || "Deno";         // 名称
const PORT: number = parseInt(Deno.env.get("PORT") || "8987"); 

const SETTINGS: Settings = {
  UUID,
  LOG_LEVEL: "none",
  BUFFER_SIZE: 2048,
  XPATH: `%2F${XPATH}`,
  MAX_BUFFERED_POSTS: 30,
  MAX_POST_SIZE: 1000000,
  SESSION_TIMEOUT: 30000,
  CHUNK_SIZE: 1024 * 1024,
  TCP_NODELAY: true,
  TCP_KEEPALIVE: true,
};

function validate_uuid(left: Uint8Array, right: Uint8Array): boolean {
  for (let i = 0; i < 16; i++) {
    if (left[i] !== right[i]) return false;
  }
  return true;
}

function concat_typed_arrays(...args: Uint8Array[]): Uint8Array {
  let len = 0;
  for (const a of args) len += a.length;
  const r = new Uint8Array(len);
  let offset = 0;
  for (const a of args) {
    r.set(a, offset);
    offset += a.length;
  }
  return r;
}

function parse_uuid(uuid: string): Uint8Array {
  uuid = uuid.replaceAll("-", "");
  const r = new Uint8Array(16);
  for (let index = 0; index < 16; index++) {
    r[index] = parseInt(uuid.substr(index * 2, 2), 16);
  }
  return r;
}

async function read_vless_header(
  reader: ReadableStreamDefaultReader<Uint8Array>,
  cfg_uuid_str: string
): Promise<{
  hostname: string;
  port: number;
  data: Uint8Array;
  resp: Uint8Array;
}> {
  let readed_len = 0;
  let header = new Uint8Array();

  async function inner_read_until(offset: number): Promise<void> {
    while (readed_len < offset) {
      const { value, done } = await reader.read();
      if (done) throw new Error("header length too short");
      header = concat_typed_arrays(header, value!);
      readed_len += value!.length;
    }
  }

  await inner_read_until(1 + 16 + 1);

  const version = header[0];
  const uuid = header.slice(1, 1 + 16);
  const cfg_uuid = parse_uuid(cfg_uuid_str);
  if (!validate_uuid(uuid, cfg_uuid)) {
    throw new Error("invalid UUID");
  }
  const pb_len = header[1 + 16];
  const addr_plus1 = 1 + 16 + 1 + pb_len + 1 + 2 + 1;
  await inner_read_until(addr_plus1 + 1);

  const cmd = header[1 + 16 + 1 + pb_len];
  const COMMAND_TYPE_TCP = 1;
  if (cmd !== COMMAND_TYPE_TCP) {
    throw new Error(`unsupported command: ${cmd}`);
  }

  const port = (header[addr_plus1 - 1 - 2] << 8) + header[addr_plus1 - 1 - 1];
  const atype = header[addr_plus1 - 1];

  const ADDRESS_TYPE_IPV4 = 1;
  const ADDRESS_TYPE_STRING = 2;
  const ADDRESS_TYPE_IPV6 = 3;
  let header_len = -1;
  if (atype === ADDRESS_TYPE_IPV4) {
    header_len = addr_plus1 + 4;
  } else if (atype === ADDRESS_TYPE_IPV6) {
    header_len = addr_plus1 + 16;
  } else if (atype === ADDRESS_TYPE_STRING) {
    header_len = addr_plus1 + 1 + header[addr_plus1];
  }
  if (header_len < 0) {
    throw new Error("read address type failed");
  }
  await inner_read_until(header_len);

  const idx = addr_plus1;
  let hostname = "";
  if (atype === ADDRESS_TYPE_IPV4) {
    hostname = Array.from(header.slice(idx, idx + 4))
      .map((b) => b.toString())
      .join(".");
  } else if (atype === ADDRESS_TYPE_STRING) {
    hostname = new TextDecoder().decode(header.slice(idx + 1, idx + 1 + header[idx]));
  } else if (atype === ADDRESS_TYPE_IPV6) {
    hostname = Array.from({ length: 8 }, (_, i) =>
      ((header[idx + i * 2] << 8) + header[idx + i * 2 + 1]).toString(16)
    ).join(":");
  }

  if (!hostname) {
    throw new Error("parse hostname failed");
  }

  return {
    hostname,
    port,
    data: header.slice(header_len),
    resp: new Uint8Array([version, 0]),
  };
}

async function parse_header(
  uuid_str: string,
  client: { readable: ReadableStream<Uint8Array> }
): Promise<any> {
  const reader = client.readable.getReader();
  try {
    const vless = await read_vless_header(reader, uuid_str);
    return vless;
  } catch (err) {
    throw new Error(`read vless header error: ${err.message}`);
  } finally {
    reader.releaseLock();
  }
}

async function connect_remote(hostname: string, port: number): Promise<Deno.Conn> {
  const timeout = 8000;
  try {
    const conn = await Deno.connect({ hostname, port });
    return conn;
  } catch (err) {
    throw err;
  }
}

function pipe_relay() {
  async function pump(
    src: ReadableStream<Uint8Array>,
    dest: WritableStream<Uint8Array>,
    first_packet: Uint8Array
  ): Promise<void> {
    if (first_packet.length > 0) {
      const writer = dest.getWriter();
      await writer.write(first_packet);
      writer.releaseLock();
    }

    try {
      await src.pipeTo(dest, {
        preventClose: false,
        preventAbort: false,
        preventCancel: false,
        signal: AbortSignal.timeout(SETTINGS.SESSION_TIMEOUT),
      });
    } catch (err) {
      throw err;
    }
  }
  return pump;
}

function relay(
  cfg: Settings,
  client: { readable: ReadableStream<Uint8Array>; writable: WritableStream<Uint8Array> },
  remote: Deno.Conn,
  vless: { data: Uint8Array; resp: Uint8Array }
): void {
  const pump = pipe_relay();
  let isClosing = false;

  const remoteStream = {
    readable: remote.readable,
    writable: remote.writable,
  };

  function cleanup(): void {
    if (!isClosing) {
      isClosing = true;
      try {
        remote.close();
      } catch (err) {
      }
    }
  }

  const uploader = pump(client.readable, remoteStream.writable, vless.data)
    .catch((err) => {
    })
    .finally(cleanup);

  const downloader = pump(remoteStream.readable, client.writable, vless.resp)
    .catch((err) => {
    });

  downloader.finally(() => uploader).finally(cleanup);
}

const sessions = new Map<string, Session>();

class Session {
  uuid: string;
  nextSeq: number = 0;
  downstreamStarted: boolean = false;
  lastActivity: number = Date.now();
  vlessHeader: any = null;
  remote: Deno.Conn | null = null;
  initialized: boolean = false;
  responseHeader: Uint8Array | null = null;
  headerSent: boolean = false;
  bufferedData: Map<number, Uint8Array> = new Map();
  cleaned: boolean = false;
  pendingPackets: Uint8Array[] = [];
  currentStreamRes: { writable: WritableStream<Uint8Array> } | null = null;
  pendingBuffers: Map<number, Uint8Array> = new Map();

  constructor(uuid: string) {
    this.uuid = uuid;
  }

  async initializeVLESS(firstPacket: Uint8Array): Promise<boolean> {
    if (this.initialized) return true;

    try {
      const readable = new ReadableStream({
        start(controller) {
          controller.enqueue(firstPacket);
          controller.close();
        },
      });

      const client = {
        readable,
        writable: new WritableStream(),
      };

      this.vlessHeader = await parse_header(SETTINGS.UUID, client);
      this.remote = await connect_remote(this.vlessHeader.hostname, this.vlessHeader.port);
      this.initialized = true;
      return true;
    } catch (err) {
      return false;
    }
  }

  async processPacket(seq: number, data: Uint8Array): Promise<boolean> {
    try {
      this.pendingBuffers.set(seq, data);

      while (this.pendingBuffers.has(this.nextSeq)) {
        const nextData = this.pendingBuffers.get(this.nextSeq)!;
        this.pendingBuffers.delete(this.nextSeq);

        if (!this.initialized && this.nextSeq === 0) {
          if (!await this.initializeVLESS(nextData)) {
            throw new Error("Failed to initialize VLESS connection");
          }
          this.responseHeader = this.vlessHeader.resp;
          await this._writeToRemote(this.vlessHeader.data);

          if (this.currentStreamRes) {
            this._startDownstreamResponse();
          }
        } else {
          if (!this.initialized) {
            continue;
          }
          await this._writeToRemote(nextData);
        }

        this.nextSeq++;
      }

      if (this.pendingBuffers.size > SETTINGS.MAX_BUFFERED_POSTS) {
        throw new Error("Too many buffered packets");
      }

      return true;
    } catch (err) {
      throw err;
    }
  }

  startDownstream(res: { writable: WritableStream<Uint8Array> }): boolean {
    this.currentStreamRes = res;
    if (this.initialized && this.responseHeader) {
      this._startDownstreamResponse();
    }
    return true;
  }

  async _writeToRemote(data: Uint8Array): Promise<void> {
    if (!this.remote) {
      throw new Error("Remote connection not available");
    }
    const writer = this.remote.writable.getWriter();
    await writer.write(data);
    writer.releaseLock();
  }

  _startDownstreamResponse(): void {
    if (!this.currentStreamRes || !this.responseHeader) return;

    try {
      const writer = this.currentStreamRes.writable.getWriter();
      writer.write(this.responseHeader);
      this.headerSent = true;
      writer.releaseLock();

      this.remote!.readable.pipeTo(this.currentStreamRes.writable).catch((err) => {
      });
    } catch (err) {
      this.cleanup();
    }
  }

  cleanup(): void {
    if (!this.cleaned) {
      this.cleaned = true;
      if (this.remote) {
        this.remote.close();
        this.remote = null;
      }
      this.initialized = false;
      this.headerSent = false;
    }
  }
}

let ISP = "";

try {
  const response = await fetch("https://speed.cloudflare.com/meta");
  if (!response.ok) {
    throw new Error(`HTTP error! status: ${response.status}`);
  }
  const data = await response.json() as {
    country: string; 
    asOrganization: string;
  };
  ISP = `${data.country}-${data.asOrganization}`.replace(/ /g, "_");
} catch (err) {
  ISP = "unknown";
}

let IP = DOMAIN;
if (!DOMAIN) {
  try {
    const p = Deno.run({
      cmd: ["curl", "-s", "--max-time", "2", "ipv4.ip.sb"],
      stdout: "piped",
    });
    const output = await p.output();
    IP = new TextDecoder().decode(output).trim();
  } catch (err) {
    try {
      const p = Deno.run({
        cmd: ["curl", "-s", "--max-time", "1", "ipv6.ip.sb"],
        stdout: "piped",
      });
      const output = await p.output();
      IP = `[${new TextDecoder().decode(output).trim()}]`;
    } catch (ipv6Err) {
      IP = "localhost";
    }
  }
}

function generatePadding(min: number, max: number): string {
  const length = min + Math.floor(Math.random() * (max - min));
  return btoa(Array(length).fill("X").join(""));
}

serve(
  async (req: Request): Promise<Response> => {
    const url = new URL(req.url);
    const path = url.pathname;

    const headers = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST",
      "Cache-Control": "no-store",
      "X-Accel-Buffering": "no",
      "X-Padding": generatePadding(100, 1000),
    };

    if (path === "/") { 
      return new Response("Hello, World\n", 
      { status: 200, 
        headers: { "Content-Type": "text/plain" }, }); 
    } 

    if (path === `/${SUB_PATH}`) {
      const vlessURL = `vless://${UUID}@${IP}:443?encryption=none&security=tls&sni=${IP}&fp=chrome&allowInsecure=1&type=xhttp&host=${IP}&path=${SETTINGS.XPATH}&mode=packet-up#${NAME}-${ISP}`;
      const base64Content = btoa(vlessURL);
      return new Response(base64Content + "\n", {
        status: 200,
        headers: { "Content-Type": "text/plain" },
      });
    }

    const pathMatch = path.match(new RegExp(`/${XPATH}/([^/]+)(?:/([0-9]+))?$`));
    if (!pathMatch) {
      return new Response("Not Found", { status: 404 });
    }

    const uuid = pathMatch[1];
    const seq = pathMatch[2] ? parseInt(pathMatch[2]) : null;

    if (req.method === "GET" && !seq) {
      let session = sessions.get(uuid);
      if (!session) {
        session = new Session(uuid);
        sessions.set(uuid, session);
      }

      session.downstreamStarted = true;
      const { readable, writable } = new TransformStream();
      session.startDownstream({ writable });

      return new Response(readable, {
        status: 200,
        headers: {
          ...headers,
          "Content-Type": "application/octet-stream",
          "Transfer-Encoding": "chunked",
        },
      });
    }

    if (req.method === "POST" && seq !== null) {
      let session = sessions.get(uuid);
      if (!session) {
        session = new Session(uuid);
        sessions.set(uuid, session);

        setTimeout(() => {
          const currentSession = sessions.get(uuid);
          if (currentSession && !currentSession.downstreamStarted) {
            currentSession.cleanup();
            sessions.delete(uuid);
          }
        }, SETTINGS.SESSION_TIMEOUT);
      }

      const data = await req.arrayBuffer();
      const buffer = new Uint8Array(data);

      try {
        await session.processPacket(seq, buffer);
        return new Response(null, { status: 200, headers });
      } catch (err) {
        session.cleanup();
        sessions.delete(uuid);
        return new Response(null, { status: 500 });
      }
    }
    return new Response("Not Found", { status: 404 });
  },
  { port: PORT, onListen: () => {
    console.log(`Server is running on port ${PORT}`);
  } }
);
