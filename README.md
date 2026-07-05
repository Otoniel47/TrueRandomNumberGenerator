# Physical TRNG Ingestion Pipeline (.NET & ESP32-S3)

Este repositório contém a implementação de um **True Random Number Generator (TRNG)** físico de ponta a ponta. O projeto extrai o ruído térmico analógico residual de uma junção PN de semicondutor em nível de hardware e consome esse fluxo contínuo através de um pipeline de ingestão concorrente de alta performance construído em **.NET**.

O design do software foi arquitetado sob os princípios de **alta performance, baixa latência e segurança ativa**, sendo totalmente compatível com **Native AOT** (Zero Reflection, Zero JIT overhead).

---

## 🚀 Arquitetura e Diferenciais Técnicos

### 1. Camada de Hardware & Firmware (ESP32-S3)
*   **Amostragem Pura:** Captura de flutuações de microvolts utilizando o ADC nativo do ESP32-S3 configurado em resolução máxima de 12 bits (0 a 4095).
*   **Clock de Amostragem via Hardware:** Amostragem controlada em intervalos de microssegundos para evitar saturação e overhead de buffers.
*   **Abordagem Sem Amplificador Dedicado:** O código do firmware maximiza a sensibilidade do conversor analógico-digital para extrair o ruído de fundo físico do silício mesmo sem ganho operacional externo.

### 2. Camada de Ingestão & Backend (.NET)
*   **Zero-Allocation ASCII Parser (`SerialAsciiNumberParser`):** Uma máquina de estados que processa o stream binário da porta serial byte a byte. Ele reconstrói os números inteiros transmitidos sem alocar uma única string em memória, mitigando completamente a pressão sobre o Garbage Collector (GC).
*   **Monitoramento de Integridade (`EntropyHealthMonitor`):** Implementação de um monitor estatístico que descarta amostras caso detecte falhas físicas de hardware ou travamento de sinal por valores repetidos consecutivos.
*   **Desenviesamento Matemático (`VonNeumannExtractor`):** O ruído físico nativo possui viés estatístico. O pipeline aplica o algoritmo de extração de Von Neumann em tempo real na stream de bits brutos ($LSB$), descartando sequências idênticas e extraindo apenas aleatoriedade pura.
*   **SHA-256 Entropy Pooling (`Sha256EntropyPool`):** Processamento e compressão dos bits limpos em blocos de 64 bytes, aplicando hashing SHA-256 (conforme recomendações NIST SP 800-90B) para garantir difusão perfeita e efeito avalanche no token criptográfico final.
*   **Produtor-Consumidor Concorrente Desacoplado:** Uso de `System.Threading.Channels` delimitados (`BoundedChannelOptions`) com modo estrito de descarte (`DropWrite`). O worker de background que consome a serial nunca bloqueia e atua de forma totalmente assíncrona aos endpoints HTTP expostos.
*   **Segurança em Memória:** Uso explícito de `CryptographicOperations.ZeroMemory` para limpar fisicamente os buffers contendo o hash extraído logo após sua publicação no canal, impedindo ataques de inspeção de memória (Memory Dump).

---

## 🛠️ Tecnologias Utilizadas

*   **Firmware:** C++ (Arduino Framework / PlatformIO)
*   **Backend:** .NET (C#)
*   **Concorrência:** System.Threading.Channels
*   **Criptografia:** System.Security.Cryptography (SHA-256 / ZeroMemory)
*   **Serialização:** System.Text.Json (Source Generators para compatibilidade total com Native AOT)

*   <img width="1600" height="1204" alt="WhatsApp Image 2026-07-05 at 17 14 12" src="https://github.com/user-attachments/assets/2fe66a89-4252-4800-8a07-d0e18874aec4" />

