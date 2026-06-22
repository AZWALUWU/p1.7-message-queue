# Message Queue Architecture (Order Processing System)

## Analisis: Message Queue vs Direct API Call

| Karakteristik | Direct API Call (Synchronous) | Message Queue (Asynchronous) |
| :--- | :--- | :--- |
| **Koneksi (Coupling)** | **Tightly Coupled.** Service A harus tahu URL/alamat Service B secara langsung. | **Loosely Coupled.** Service A hanya mengirim pesan ke Topic/Queue, tidak peduli siapa konsumennya. |
| **Ketersediaan (Availability)** | Jika Service B mati (*down*), maka proses di Service A langsung gagal (*Single Point of Failure*). | Jika konsumen mati, pesan tetap aman tersimpan di Queue sampai konsumen aktif kembali. |
| **Performa & Latensi** | Pengguna harus menunggu seluruh rangkaian service selesai merespon (Latensi akumulatif). | Pengguna mendapat respon instan setelah data masuk queue. Proses berat diselesaikan di latar belakang. |
| **Skalabilitas** | Rentan crash jika ada lonjakan trafik (*traffic spike*) mendadak karena resource langsung habis. | Queue berfungsi sebagai *buffer* penahan beban, lalu diproses bertahap sesuai kapasitas aplikasi (*Throttling protection*). |

**Kapan menggunakan Message Queue?** Ketika sebuah proses tidak memerlukan jawaban instan secara langsung (contoh: pembuatan invoice, update inventori gudang, pengiriman email notifikasi) dan sistem membutuhkan ketahanan tinggi (*fault-tolerance*) serta performa yang responsif.
