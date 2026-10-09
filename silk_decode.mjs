// Optional silk-wasm dependency; not installed or invoked by default.
// stdin SILK -> stdout PCM WAV. No network, transcript service or log output.
try {
  const parts = [];
  let size = 0;
  for await (const part of process.stdin) {
    size += part.length;
    if (size > 4 * 1024 * 1024) throw Error();
    parts.push(part);
  }
  const { decode } = await import('silk-wasm');
  const { data } = await decode(Buffer.concat(parts), 24000);
  if (!data.length || data.length > 24000 * 2 * 60 || data.length % 2) throw Error();
  const wav = Buffer.alloc(44 + data.length);
  wav.write('RIFF', 0); wav.writeUInt32LE(36 + data.length, 4);
  wav.write('WAVEfmt ', 8); wav.writeUInt32LE(16, 16);
  wav.writeUInt16LE(1, 20); wav.writeUInt16LE(1, 22);
  wav.writeUInt32LE(24000, 24); wav.writeUInt32LE(48000, 28);
  wav.writeUInt16LE(2, 32); wav.writeUInt16LE(16, 34);
  wav.write('data', 36); wav.writeUInt32LE(data.length, 40);
  Buffer.from(data).copy(wav, 44);
  process.stdout.write(wav);
} catch { process.exitCode = 1; }
