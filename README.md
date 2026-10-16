# go_bizbos — WhatsApp service bizbos v2

Go + Gin + whatsmeow. Melayani app.bizbos; database MySQL sama dengan app.bizbos (tabel `wa_*` dibuat migration Laravel). Sesi WhatsApp (kunci enkripsi) disimpan terpisah di `data/session.db` (SQLite).

## Struktur

```
cmd/server          main: merangkai semua bagian
internal/config     env + validasi rahasia
internal/store      koneksi MySQL + model tabel wa_*
internal/auth       verifikasi request bizbos (HMAC + nonce) & token browser
internal/device     manager device per branch: QR, kode pairing, reconnect, logout, ban
internal/message    pesan masuk, receipt, normalisasi @lid, opt-out
internal/media      unduh media saat diminta (lazy) + minta ulang ke HP bila kedaluwarsa, simpan upload
internal/sender     antrean kirim per device: manual didahulukan, blast dengan jeda/batas/jam tenang
internal/realtime   SSE hub per branch
internal/webhook    outbox webhook ke bizbos dengan retry
internal/notify     satu pintu: kirim event ke SSE + webhook
internal/api        router & handler tipis
```

## Menjalankan (lokal)

```powershell
copy .env.example .env      # isi DB_* dan tiga rahasia WA_*
go mod tidy
go run ./cmd/server
```

Di app.bizbos: isi `WA_SERVICE_URL`, `WA_PUBLIC_URL`, `WA_API_SECRET`, `WA_ACCESS_SECRET`, `WA_WEBHOOK_SECRET` (nilai sama dengan go_bizbos), lalu `php artisan migrate`.

## Keamanan

- Go tidak punya login/JWT. Semua aksi lewat bizbos dengan header `X-Bizbos-Timestamp`, `X-Bizbos-Nonce`, `X-Bizbos-Branch`, `X-Bizbos-Signature`.
  Signature = `hex(HMAC-SHA256(WA_API_SECRET, ts \n nonce \n METHOD \n uri \n branch_id \n sha256(body)))`, berlaku 5 menit, nonce sekali pakai. Body diverifikasi sebelum diproses.
- Setiap device/chat/pesan/blast dicek `branch_id`-nya; UUID bukan izin.
- Browser hanya membuka `/realtime` (SSE) dan `/media/:id` dengan token singkat (maks 15 menit) buatan bizbos, ditandatangani `WA_ACCESS_SECRET`.
- Webhook ke bizbos: `X-Bizbos-Signature = hex(HMAC-SHA256(WA_WEBHOOK_SECRET, ts + "." + body))`, `X-Bizbos-Delivery` untuk dedup.
- Tiga rahasia wajib berbeda dan ≥32 karakter (server menolak start).
- Media: tipe ditentukan dari isi file, nama file acak, `nosniff`, `sandbox`, selain gambar/video/audio selalu diunduh.
- Listen di `127.0.0.1`; publikkan hanya `/realtime`, `/media`, dan `/avatar` lewat Nginx.

## Blast (banyak tenant sekaligus)

- bizbos membuat `wa_campaigns` (draft) + baris `wa_outbox` (priority 10), lalu `POST /v1/campaigns/:id/start`.
- Satu worker per device, batas paralel global `MAX_PARALLEL_SENDS`.
- Jeda acak `BLAST_MIN_DELAY`–`BLAST_MAX_DELAY` detik + indikator mengetik; chat manual tetap jalan di sela jeda.
- Batas harian per device (`daily_cap` atau `BLAST_DAILY_CAP`), nomor baru dipanaskan 20/40/60… per hari.
- Jam tenang `QUIET_START_HOUR`–`QUIET_END_HOUR` (blast ditahan, manual tetap jalan).
- Nomor dicek terdaftar di WhatsApp; balasan STOP/BERHENTI masuk `wa_optouts` dan tidak dikirimi blast lagi.
- Bila WhatsApp membatasi nomor (TemporaryBan) semua blast device itu di-pause otomatis.

## API (dipanggil server bizbos)

| Method | Path | Keterangan |
|---|---|---|
| POST | /v1/devices/:id/connect | `{phone?}` kosong = QR (via SSE `device.qr` / status), diisi = kode pairing |
| GET | /v1/devices/:id/status | status, QR terakhir, online |
| POST | /v1/devices/:id/logout | keluarkan perangkat |
| POST | /v1/messages | `{device_id, to, text, quoted_wa_id?, client_ref?}` |
| POST | /v1/messages/media | multipart: `device_id, to, text?, type?, client_ref?, file` |
| POST | /v1/messages/poll | `{device_id, to, question, options[], selectable_count, client_ref?}` |
| POST | /v1/uploads | multipart `file` → `upload_id` untuk blast |
| POST | /v1/chats/:id/presence | minta status online/mengetik kontak (dikirim lewat SSE `presence`) |
| GET | /v1/devices/:id/groups | daftar grup WhatsApp yang diikuti |
| GET | /v1/devices/:id/groups/:jid | anggota grup (nomor HP bila tidak disembunyikan) |
| POST | /v1/chats/:id/read | tandai dibaca (kirim read receipt) |
| POST | /v1/campaigns/:id/start\|pause\|cancel | kontrol blast |

Browser (token singkat dari bizbos): `GET /realtime` (scope `sse`), `GET /media/:id` (scope `media`), `GET /avatar/:chat_id` (scope `avatar`, foto profil disimpan 24 jam).

Event SSE/webhook: `device.status`, `device.qr` (SSE saja), `message.new`, `message.status`, `message.media`, `outbox.status`, `campaign.status`, `presence` dan `poll.vote` (SSE saja).

## Nginx (contoh, wa.bizbos.id)

```nginx
location ~ ^/(realtime|media/|avatar/) {
    proxy_pass http://127.0.0.1:8090;
    proxy_http_version 1.1;
    proxy_set_header Connection "";
    proxy_buffering off;
    proxy_read_timeout 3700s;
}
location / { return 404; }
```
