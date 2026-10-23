# PrimeGrid

Implementação em Python 3.11+. A Fase A contém o núcleo sequencial puro;
a Fase B acrescenta distribuição gRPC, eleição Bully na partida e SQLite local.
Recuperação automática de falhas será validada na Fase C e benchmarks na Fase D.

## Instalação e testes

Windows (PowerShell):

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
.\.venv\Scripts\python.exe scripts/gen_proto.py
.\.venv\Scripts\python.exe -m pytest
```

Linux:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -e '.[dev]'
.venv/bin/python scripts/gen_proto.py
.venv/bin/python -m pytest
```

## Núcleo sequencial

```python
from math import isqrt
from primegrid.sieve import base_primes, count_range, sieve_segment, split_segments

assert count_range(1, 1_000_000) == 78_498
base = base_primes(isqrt(30))  # uma base por job, compartilhada entre segmentos
results = [
    sieve_segment(s.start, s.end, base, True, chunk_id=s.chunk_id)
    for s in split_segments(1, 30, chunk_size=7)
]
primes = [p for result in results for p in result.primes]
```

Os extremos são inclusivos. IDs dos segmentos começam em zero. O modo count
retorna a contagem e não constrói listas de primos dos segmentos; em list,
`SegmentResult.primes` contém a lista ordenada. A base fornecida a
`sieve_segment` deve conter todos os primos ordenados até `isqrt(R)`.
`count_range` calcula e reutiliza essa base automaticamente.

Limites: `0 <= start <= end <= 10**12`, até `10**9` inteiros por intervalo,
até `1_000_000` inteiros em list, tamanho de segmento inteiro positivo.

Instale em cada computador, com sua própria cópia dos arquivos e ambiente
virtual. Dependências de execução: grpcio e protobuf. O extra dev inclui
grpcio-tools, pytest e pytest-asyncio. Para somente executar, instale com
`python -m pip install -e .` dentro do ambiente virtual.

Os módulos protobuf gerados estão incluídos. Após alterar
`primegrid/proto/primegrid.proto`, regenere com `scripts/gen_proto.py` usando a
versão de grpcio-tools fixada no projeto. Não edite os módulos gerados.

## Arquitetura e protocolo

- `sieve.py` e `models.py`: algoritmo puro, modelos e validação.
- `leader.py`: fila determinística com atribuição sob demanda, um segmento por
  worker, agregação e rejeição de duplicatas conflitantes.
- `worker.py`: solicita tarefas; usa um ProcessPoolExecutor com um processo
  por nó, reaproveitado junto com os primos-base durante o job.
- `storage.py`: SQLite local, commits por resultado e chave `(job_id,chunk_id)`;
  listas compactadas com JSON + zlib somente em modo list.
- `rpc.py`, `node.py` e `proto/`: transporte tipado, deadlines, grpc.aio,
  sincronização da especificação e ciclo de vida.
- `election.py`: Bully manual. Envia Election a IDs maiores, aguarda ACK e
  Coordinator e repete em timeout. O maior ID ativo assume na partida,
  inclusive se iniciar depois dos demais.

O líder também calcula. Clientes consultam qualquer nó e redirecionam ao líder
via GetStatus. StartJob persiste a especificação e só confirma após ACK de
SyncJob de pelo menos outro nó. Workers persistem antes de SubmitResult;
o líder persiste antes do ACK e soma cada segmento uma vez. Entregas sem ACK
são repetidas com backoff limitado a 2 segundos. Status verifica peers com
deadlines; não há monitor contínuo de falhas nesta fase.

RPCs funcionais: GetStatus, StartJob, SyncJob, RequestTask, SubmitResult,
Election, Coordinator e GetResult. Heartbeat e GetSnapshot estão definidos no
contrato e retornam UNIMPLEMENTED até a Fase C. Há um job ativo por cluster.

## Demonstração local: dois processos

Abra dois terminais na raiz do projeto. Windows:

```powershell
# Terminal 1
.\.venv\Scripts\python.exe -m primegrid node --config config/nodes.local.json --id 1
```

```powershell
# Terminal 2
.\.venv\Scripts\python.exe -m primegrid node --config config/nodes.local.json --id 2
```

No Linux, use `.venv/bin/python` com os mesmos argumentos. A configuração
local usa 127.0.0.1 com portas 50051 e 50052. Aguarde o evento coordinator
com leader_id 2. Em um terceiro terminal:

```powershell
.\.venv\Scripts\python.exe -m primegrid status --node 127.0.0.1:50051
.\.venv\Scripts\python.exe -m primegrid submit --node 127.0.0.1:50051 --start 1 --end 1000000 --chunk-size 5000 --mode count
.\.venv\Scripts\python.exe -m primegrid status --node 127.0.0.1:50051
.\.venv\Scripts\python.exe -m primegrid result --node 127.0.0.1:50051
```

Resultado esperado ao concluir: state completed, prime_count 78498,
200 segmentos concluídos, líder 2. Os comandos imprimem JSON. Consulte
`python -m primegrid --help` e o --help de cada comando para os argumentos.

Após concluir esse job, experimente list:

```powershell
.\.venv\Scripts\python.exe -m primegrid submit --node 127.0.0.1:50051 --start 1 --end 30 --chunk-size 7 --mode list
# Espere status indicar completed antes de exportar.
.\.venv\Scripts\python.exe -m primegrid result --node 127.0.0.1:50051 --csv resultado.csv
```

CSV: uma coluna prime, valores ordenados. A exportação exige job list
concluído. Em count, result mostra quantidade, duração e total de segmentos.

## Dois computadores reais na LAN

1. Instale em cada computador. Copie config/nodes.example.json para
   config/nodes.json e substitua os IPs pelos IPv4 reais da LAN.
2. Mantenha a mesma lista de IDs, hosts e portas em ambas as cópias.
   Exemplo: computador A, ID 1, 192.168.1.10:50051; computador B, ID 2,
   192.168.1.11:50051. Os endereços anunciados devem ser acessíveis pelos peers.
3. Permita TCP de entrada na porta escolhida no firewall da rede privada.
   O servidor faz bind em 0.0.0.0:<port>. Não exponha o serviço à Internet.
4. Execute na raiz do projeto. data_dir é relativo a esse diretório.
   SQLite é local a cada máquina: não use pasta de rede nem sincronize
   bancos entre computadores.

Windows — computador A:

```powershell
New-Item -ItemType Directory -Force logs | Out-Null
.\.venv\Scripts\python.exe -m primegrid node --config config/nodes.json --id 1 | Tee-Object -FilePath logs/node_1.jsonl
```

Windows — computador B:

```powershell
New-Item -ItemType Directory -Force logs | Out-Null
.\.venv\Scripts\python.exe -m primegrid node --config config/nodes.json --id 2 | Tee-Object -FilePath logs/node_2.jsonl
```

Linux — execute no computador A e B respectivamente:

```bash
mkdir -p logs
.venv/bin/python -m primegrid node --config config/nodes.json --id 1 | tee logs/node_1.jsonl
```

```bash
mkdir -p logs
.venv/bin/python -m primegrid node --config config/nodes.json --id 2 | tee logs/node_2.jsonl
```

De um terminal adicional em qualquer computador, Windows:

```powershell
.\.venv\Scripts\python.exe -m primegrid status --node 192.168.1.10:50051
.\.venv\Scripts\python.exe scripts/verify_cluster.py --config config/nodes.json --output logs/lan-validation.json
```

Linux:

```bash
.venv/bin/python -m primegrid status --node 192.168.1.10:50051
.venv/bin/python scripts/verify_cluster.py --config config/nodes.json --output logs/lan-validation.json
```

O verificador exige dois nós acessíveis, verifica o maior ID ativo como líder,
submete [1, 1_000_000] e compara a contagem distribuída com o crivo sequencial
usando o mesmo chunk_size. Execute sem outro job ativo. Guarde o JSON e os logs
de ambos os computadores como evidência da LAN; endereços distintos por si só
não comprovam que eram máquinas físicas diferentes.

Para submissões livres, troque 127.0.0.1:50051 pelos IPs reais nos comandos
submit/status/result. Para um terceiro nó, inclua seu ID e endpoint na
configuração de todos antes de iniciar. Não há membros dinâmicos.

## Logs, testes e estado da validação

Cada linha de log contém JSON com horário UTC, ID do nó e dados do job ou
segmento. Eventos principais: election_started, election_ack, coordinator,
job_synced, job_started, task_assigned, chunk_started, result_persisted,
result_accepted, result_ack e job_completed. Não são registrados primos
individuais. Bancos: data/node_<id>.sqlite3.

```powershell
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\python.exe -m pytest -q tests/test_integration.py --basetemp=.tmp/integration
```

No Linux, use .venv/bin/python. A integração sobe dois servidores em processos
reais e portas livres, usa gRPC e subprocessos CPU. Verifica contagem,
participação de ambos os nós, persistência, duplicação idempotente, segmentos
contíguos, eleição, redirecionamento, CSV, jobs sucessivos e recusa de início
sem ACK de outro nó. Os logs ficam em subpastas do diretório --basetemp.

A validação local não substitui o teste em dois computadores reais.
Pendente: executar o roteiro LAN e guardar as evidências em duas máquinas.

## Falhas e limites da entrega atual

Para demonstrar uma falha, submeta um job maior e pressione Ctrl+C no terminal
de um worker ou do líder. Na Fase B, o job pode ficar parado: não há ainda
heartbeats, leases renováveis, reatribuição, snapshots ou recuperação automática
após crash/reinício. Os parâmetros heartbeat (2 s), suspeita (6 s) e lease
(30 s) estão na configuração para a Fase C. O cenário de falha não está aprovado.

Bancos preservam especificações e resultados; resultados já concluídos podem
ser consultados após reiniciar o líder original. Retomada de jobs em curso e
sincronização de resultados de worker reiniciado serão tratadas na Fase C.
O modelo é LAN com crash-stop e perdas temporárias de comunicação, sem garantia
de líder único ou consistência sob partições arbitrárias. Não há autenticação,
múltiplos jobs simultâneos, REST ou interface gráfica.

O comando benchmark será implementado na Fase D. Os integrantes escrevem os
relatórios acadêmicos; código, logs e métricas fornecem evidências.
