import os
import subprocess
import shutil
import speech_recognition as sr
from threading import Thread, current_thread
from datetime import datetime, timedelta
import mysql.connector
import queue
import re
from difflib import SequenceMatcher
import requests
import time
import wave
from threading import Lock
import logging

# ==============================================
# CONFIGURAÇÃO DOS LOGGS
# ============================================== 

### Por que tanta ênfase nos LOGS? O objetivo é usarmos os resultados não só do terminal, mas de arquivos separados para identificar inconsistências na transcrição, gravação e etc...


# Configura o módulo logging para registrar mensagens de log em um arquivo chamado processamento.log.

logging.basicConfig(level=logging.INFO, filename='processamento.log', filemode='a',
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Objetivo: Cria um objeto Lock do módulo threading para sincronizar o acesso a recursos compartilhados entre múltiplas threads.
# O lock é utilizado para garantir que apenas uma thread por vez possa adicionar itens à fila de transcrição (queue_transcricao), evitando condições de corrida.
lock = Lock() 

last_log_time = {}
last_capture_log_time = {}

LOG_DIR = "logs"


LOG_INTERVAL = timedelta(minutes=5)
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)
    
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

### def para LOGs no consolo, e no arquivo gerado com logs. 

def log_message(message, level="INFO"):
    current_time = datetime.now()
    with open(f"{LOG_DIR}/capture_log.txt", "a") as log_file:
        log_file.write(f"{current_time} - [{level}] {message}\n")
    print(f"[{level}] {message}")
    

def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()

### Procura o binário do ffmpeg no PATH ou em locais comuns do Windows.
def find_ffmpeg():
    ffmpeg_bin = shutil.which('ffmpeg')
    if ffmpeg_bin:
        return ffmpeg_bin

    common_locations = [
        r'C:\ffmpeg\ffmpeg.exe',
        r'C:\Program Files\ffmpeg\bin\ffmpeg.exe',
        r'C:\Program Files (x86)\ffmpeg\bin\ffmpeg.exe',
    ]
    for path in common_locations:
        if os.path.isfile(path):
            return path

    return None

### Função de auxílio na transcrição, aqui formalizamos algumas alterações básicas que auxiliarão a detecção a partir da transcrição
# O objetivo é normalizar os 0800 e evitar


def normalize_text(text):
    return text.replace(' ', '').replace('-', '').lower()


def parse_0800(value):
    normalized = normalize_text(value)
    match = re.match(r'^(0800\d{7})', normalized)
    return match.group(1) if match else None


def is_valid_0800(value):
    return parse_0800(value) is not None


# ==============================================
# BLOCO - Registro
# ============================================== 

##### Registro de detecção no 'historico_emisorras' 

# 1° Iniciamos uma conexão de consulta somente, a ideia é verificar a última inserção referente a uma EMISSORA e CLIENTE. 

def registrar_deteccao(fone, emissora, emi_cliente=None):
    try:
        conexao = mysql.connector.connect(
            host=os.getenv('ALPES_DB_HOST', '82.29.62.54'),
            database=os.getenv('ALPES_DB_NAME', 'alpes'),
            user=os.getenv('ALPES_DB_USER', 'alpesdados'),
            password=os.getenv('ALPES_DB_PASSWORD', '')
        )
        cursor = conexao.cursor()
        query_check = """
        SELECT dtAcao FROM historico_emissoras 
        WHERE fone = %s AND emissora = %s 
        ORDER BY dtAcao DESC LIMIT 1;
        """
        cursor.execute(query_check, (fone, emissora))
        last_record = cursor.fetchone()

        # 2° Com base na última inserção, uma cláusula se torna essencial para evitar registros duplicadaos no banco
        # Ela verifica se a última inserção (para cliente, e emissora) ocorreu em menos de 5 min, caso sim, o registro atual detectado NÃO será 'subido' ao banco.  
        
        if last_record:
            last_detection_time = last_record[0]
            if datetime.now() - last_detection_time < timedelta(minutes=5):
                print(f"[LOG] Número {fone} já detectado em {emissora} nos últimos 5 minutos. Ignorando...")
                return
        
        # 3° Por fim, as condições de injection no SQL são estipuladas, e caso a cláusula de última inserção não seja TRUE, registramos a última entrada no banco.  
        
        query_insert = """
        INSERT INTO historico_emissoras (dtAcao, fone, emissora, emiCliente)
        VALUES (%s, %s, %s, %s);
        """
        data_atual = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        valores = (data_atual, fone, emissora, emi_cliente)
        cursor.execute(query_insert, valores)
        conexao.commit()
        print(f"[LOG] Registro inserido no banco de dados: {valores}")
        cursor.close()
        conexao.close()
    except mysql.connector.Error as err:
        print(f"[ERRO] Erro ao conectar ao MySQL ou inserir dados: {err}")
        

# ==============================================
# 1° BLOCO - Monitoramento
# ==============================================

##### Coletar streaming da API e 0800 do banco de dados
# radios_stream (): URLs Streaming / nomes_alpes
# a partir do radios_stream (), percorre-se a gradeOPEC fornecendo o dicionário final 'radios_dinamico (URLs + 0800)

def carregar_0800_dinamico():
    """Obtém URLs de streaming e nomes padronizados diretamente da API e números 0800 do banco de dados"""
    try:
        api_url = "https://radiosbrasil.com.br/wp-json/api/listagem/radios/"
        headers = {
            "Authorization": f"Bearer {os.getenv('RADIOS_API_TOKEN', '')}"
        }

        response = requests.get(api_url, headers=headers)
        if response.status_code != 200:
            print(f"[ERRO] Falha ao acessar a API: {response.status_code}")
            return None, None

        data = response.json()

        ## Com base na entradas da Radios Brasil, criamos um dicionario (radios_stream) de acordo com a seguinte estrutura: 
        #  "TUPI": "https://linkradioTUPI.com/Live"
        #  "DIFUSORA MANAUS": "https://linkradioDIFUSORA.com/Live"
        
        radios_stream = {}
        for radio in data:
            nome_emissora = radio.get("campos", {}).get("nome_emissora_alpes", "").upper()
            streaming_url = radio.get("campos", {}).get("stream", "")
            
            if nome_emissora and streaming_url and len(streaming_url) > 10:
                radios_stream[nome_emissora] = streaming_url

        print(f"[LOG] URLs de streaming carregadas para {len(radios_stream)} emissoras.")
        
        # Aqui abrimos a conexão com o banco para extração dos 0800
        
        conexao = mysql.connector.connect(
            host=os.getenv('ALPES_DB_HOST', '82.29.62.54'),
            database=os.getenv('ALPES_DB_NAME', 'alpes'),
            user=os.getenv('ALPES_DB_USER', 'alpesdados'),
            password=os.getenv('ALPES_DB_PASSWORD', '')
        )
        cursor = conexao.cursor()
        data_atual_full = datetime.now().strftime('%Y-%m-%d')

        ## Consulta para os números 0800 usando os nomes das emissoras padronizados
        query = f"""
        SELECT emissora, canal0800, cliente
        FROM gradeOPEC 
        WHERE emissora IN ({','.join(['%s'] * len(radios_stream))})
        AND dtInsercao = %s
        AND canal0800 IS NOT NULL
        """

        params = [*radios_stream.keys(), data_atual_full]

        print(f"[LOG] Consultando gradeOPEC com dtInsercao={data_atual_full} para {len(radios_stream)} emissoras.")
        cursor.execute(query, tuple(params))

        rows = cursor.fetchall()
        print(f"[LOG] {len(rows)} registros retornados da gradeOPEC.")
        
        ## Geração de um dicionário no mesmo formato do radios_stream, mas para MIDIA: 0800 
        
        radios_dinamicos = {}
        radios_clientes = {}
        for emissora, canal0800, cliente in rows:
            parsed = parse_0800(canal0800)
            if not parsed:
                print(f"[LOG] Ignorando valor inválido de 0800 para '{emissora}': {canal0800}")
                continue
            if emissora not in radios_dinamicos:
                radios_dinamicos[emissora] = []
            radios_dinamicos[emissora].append(parsed)
            if emissora not in radios_clientes:
                radios_clientes[emissora] = cliente.strip() if cliente else None
        
        # Remoção duplicatas
        for key in radios_dinamicos.keys():
            radios_dinamicos[key] = list(set(radios_dinamicos[key]))

        cursor.close()
        conexao.close()
        
        print(f"[LOG] 0800 carregados para o dia {data_atual_full}:")
        for radio, numeros in radios_dinamicos.items():
            print(f"  - {radio}: {numeros}")

        return radios_dinamicos, radios_stream, radios_clientes

    except Exception as err:
        print(f"[ERRO] {err}")
        return None, None, None


# ==============================================
# BLOCO - Detecção
# ==============================================


#### Função para detecção regex - se a transcrição bate em até 80% de similaridade com o dicionário, logo haverá registro

def encontrar_numero(transcription, radios_dinamicos, radio_name):
    
    # Definimos condições de normalização de texto. O intuito é tentar 'limpar' possíveis transcrições que fujam demais dos padrões detectáveis:
    
    # Detalhes da Regex:
    # 0800: Inicia com "0800".
    # [\s\-]?: Pode haver um espaço ou hífen opcional.
    # \d{3}: Três dígitos numéricos.
    # [\s\-]?: Outro espaço ou hífen opcional.
    # \d{4}: Quatro dígitos numéricos. 
     
    regex_0800 = r'0800[\s\-]?\d{3}[\s\-]?\d{4}'
    
    # Ação: Encontra todas as ocorrências que correspondem à expressão regular na transcrição 
    # /!\ NÓS 'SALVAMOS' TODOS OS 0800 QUE SE APLICAM A NORMALIZAÇÃO DE TEXTO ACIMA APLICADA
    
    matches = re.findall(regex_0800, transcription)
    
    # Aplicamos a normalize_text () anteriormente definida
    
    normalized_matches = [normalize_text(match) for match in matches]

    # Recuperamos os '0800' especificados na nossa definição  
    expected_numbers = radios_dinamicos.get(radio_name, [])

    
    for found in normalized_matches:
        for expected in expected_numbers:
            if similar(found, expected) > 0.8:
                print(f"[LOG] Número {found} detectado com sucesso em {radio_name}! Substituído pelo número exato {expected}")
                return expected
    return None


#### transcrição da fila de audio e aplicação da função 'encontrar_numero' para detecção de palavras-chave e 'registrar_deteccao' para registro da mesma, respeitando as condições

# ==============================================
# BLOCO - Transcrição           
# ==============================================

def transcrever_audio(queue_transcricao, recognizer, radios_dinamicos, radios_clientes, window_time):
    
    ### Acessamos o ultimo registro da emissora analisada atualmente na Thread,  
    last_detection_time = {}

    # Em loop constante 
    while True:
        # Acessa o registro na fila
        audio_file, radio_name = queue_transcricao.get()

        if os.path.exists(audio_file):
            try:
                # Analisa o tamanho do arquivo, se menor que o tamanho minimo (1kb), é eliminado.
                if os.path.getsize(audio_file) < 1024:
                    print(f"[LOG] Arquivo de áudio {audio_file} muito pequeno. Removendo...")
                    os.remove(audio_file)
                    queue_transcricao.task_done()
                    continue
                
                ### Se não, abre o arquivo  usando o módulo do SPEECH_RECOGNITION
                # 1° recognizer lê os dados de áudio do arquivo listado na fila. 
                # 2° Chama o reconhecimento do speech especificando a linguagem para ser aplicado
                # 3° aplicamos um filtro sobre o bloco transcriro para normalização do texto conforme nossas condições acima estipuladas.  
                
                with sr.AudioFile(audio_file) as source:
                    audio = recognizer.record(source)
                transcription = recognizer.recognize_google(audio, language='pt-BR')
                normalized_transcription = normalize_text(transcription)
                print(f"[LOG] [{radio_name}] Transcrição do áudio: {transcription}")
                
                
                ### A função de detecção é especificada dentro da função de  transcrição
                # Ou seja, quando executada a função, trancreve e detecta caso hajam similidades associadas. 
                
                numero_detectado = encontrar_numero(normalized_transcription, radios_dinamicos, radio_name)
                
                ### Caso haja detecção, realizamos um comparativo acessondo (radio e 0800) da ultima detecção do banco
                # Se a diferença entre a atual detecção for menor que 600s (10min) não registram
                
                if numero_detectado:
                    now = datetime.now()
                    last_time = last_detection_time.get((radio_name, numero_detectado))
                    
                    ### Agora se válido, chamamos a função de registro no banco
                    
                    if last_time is None or (now - last_time).total_seconds() > window_time:
                        print(f"[LOG] Palavra-chave '{numero_detectado}' detectada em [{radio_name}]!")
                        last_detection_time[(radio_name, numero_detectado)] = now
                        cliente = radios_clientes.get(radio_name) if radios_clientes else None
                        registrar_deteccao(numero_detectado, radio_name, cliente)
            except sr.UnknownValueError:
                print(f"[LOG] [{radio_name}] Google Speech Recognition não conseguiu entender o áudio")
            except sr.RequestError as e:
                print(f"[ERRO] [{radio_name}] Erro no serviço de reconhecimento de fala: {e}")
            except Exception as e:
                print(f"[ERRO] [{radio_name}] Erro ao processar o arquivo de áudio: {e}")
            finally:
                # /!\ CLÁUSULA IMPORTANTE: FINALIZADA A TRANSCRIÇÃO, DEVEMOS APAGAR O ARQUIVO GERADO, EVITANDO EXCESSOS DE ARQUIVOS GERADOS 
                # Incluir uma seção para salvarmos 
                
                if os.path.exists(audio_file):
                    os.remove(audio_file)
                    print(f"[LOG] Arquivo {audio_file} removido com sucesso.")
        queue_transcricao.task_done()
        
# ==============================================
# 2° BLOCO - Monitoramento
# ==============================================        

#### fila para captura de audio 
# parâmetros para sobreposição de áudio 
# intensificar tratamento de possíveis erros na captura
# Se válido, enviado à fila de transcrição 
# Threads de transcrição e captura 

##  radio_name: Nome da emissora de rádio (string).
##  url: URL do streaming da emissora (string).
##  queue_transcricao: Fila onde os blocos de áudio serão colocados para transcrição (objeto Queue).
##  block_duration: Duração de cada bloco de áudio em segundos (padrão: 120 segundos).
##  overlap_duration: Duração da sobreposição entre blocos de áudio em segundos (padrão: 10 segundos).
##  max_retries: Número máximo de tentativas em caso de falha na captura (padrão: 5).


def capturar_audio(radio_name, url, queue_transcricao, block_duration=120, overlap_duration=10, max_retries=5):
    retry_count = 0
    ffmpeg_exe = find_ffmpeg()

    if ffmpeg_exe is None:
        log_message(f"[ERRO] ffmpeg não encontrado. Verifique se está instalado e no PATH ou em C:\\ffmpeg.", "ERROR")
        return
    
    ### While True para capturar continuamente 
    while True:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"temp_audio_{radio_name}_{timestamp}.wav"
        ffmpeg_log_file = f"{LOG_DIR}/{radio_name}_ffmpeg_log.txt"


        ### EXPLICAÇÃO DE CADA PARÂMETRO APLICADO
        # 'ffmpeg': Chama o programa FFmpeg para processamento de áudio e vídeo.
        # '-y': Força a sobrescrita do arquivo de saída sem prompt de confirmação.
        # '-reconnect', '1': Ativa a reconexão em caso de perda de conexão.
        # '-reconnect_streamed', '1': Permite reconexão para streams contínuos.
        # '-reconnect_delay_max', '10': Define o tempo máximo de espera em segundos antes de tentar reconectar.
        # '-i', url: Especifica a URL do stream da emissora como fonte de entrada.
        # '-t', str(block_duration + overlap_duration): Define o tempo total de captura do bloco, incluindo a sobreposição.
        # '-acodec', 'pcm_s16le': Define o codec de áudio para PCM assinado de 16 bits em little-endian.
        # '-ar', '16000': Define a taxa de amostragem de áudio para 16 kHz.
        # '-ac', '1': Define o número de canais de áudio (mono).
        # output_file: Especifica o nome do arquivo onde o áudio capturado será salvo.

        command = [
            ffmpeg_exe, '-y',
            '-reconnect', '1', '-reconnect_streamed', '1', '-reconnect_delay_max', '10',
            '-i', url,
            '-af', "highpass=f=300,lowpass=f=3000,silenceremove=start_periods=1:start_threshold=-30dB", 
            '-t', str(block_duration + overlap_duration),
            '-acodec', 'pcm_s16le',
            '-ar', '44100',  # Aumente a taxa de amostragem para melhor qualidade (opcional)
            '-ac', '1',
            '-probesize', '32M', 
            '-analyzeduration', '10M',
            '-fflags', '+nobuffer', 
            '-buffer_size', '64M',
            output_file
        ]

        
        ### subprocess.run() é para executar o comando ffmpeg
        
        try:
            process = subprocess.run(
                command,
                # stdout=subprocess.PIPE: Captura a saída padrão
                stdout=subprocess.PIPE,
                # stderr=subprocess.PIPE: Captura a saída de erro, que será armazenada no arquivo de log.
                stderr=subprocess.PIPE,
                timeout=block_duration + 30
            )

            # Finalidade: Registrar informações detalhadas sobre o processo de captura para diagnóstico de problemas dentro do nosso arquivo de possíveis erros.
            with open(ffmpeg_log_file, "a", encoding="ISO-8859-1", errors="ignore") as f:
                f.write(process.stderr.decode("ISO-8859-1", errors='ignore'))

            ### Verificação de integridade do arquivo
            # Com o 'wave' abrimos o bloco de captura e lemos um frame ao menos, o intuito é validar e passar adiante para transcrição
            # Ou seja, caso haja um tamanho minimo tal, abri e valida o arquivo. Se válido, envia para fila.
            
            if os.path.exists(output_file) and os.path.getsize(output_file) > 20480:
                try:
                    with wave.open(output_file, 'rb') as wav_file:
                        wav_file.readframes(1)  # Valida o arquivo
                        
                    # Sincronização de acesso e envio para a fila de transcrição
                    with lock:
                        queue_transcricao.put((output_file, radio_name)) # ----> Envio para fila 
                        log_message(f"[{radio_name}] Bloco de áudio capturado e adicionado à fila.")
                    retry_count = 0  # Reset de contagem de retentativas após sucesso
                except wave.Error:
                    log_message(f"[{radio_name}] Arquivo de áudio corrompido removido: {output_file}")
                    os.remove(output_file)
                    continue
            else:
                log_message(f"[{radio_name}] Bloco de áudio capturado inválido ou incompleto. Tentando novamente...")
                os.remove(output_file) if os.path.exists(output_file) else None

        
        #### Processos para novas tentativas em casos de erros na captura
        # Note que há duas condições existentes para problemas: Blocos corrompidos e não captura proprimante (geralmente ocorre por erro no streaming)
        
        except subprocess.TimeoutExpired:
            log_message(f"[ERRO] Timeout na captura de áudio para {radio_name}. Tentativa {retry_count + 1} de {max_retries}.")
            retry_count += 1
        except Exception as e:
            log_message(f"[ERRO] Erro durante a captura de áudio para {radio_name}: {e}")
            retry_count += 1
        finally:
            if retry_count >= max_retries:
                log_message(f"[ERRO] Limite de tentativas alcançado para {radio_name}. Pausando por 30 segundos.")
                time.sleep(30)
                retry_count = 0  # Reset de contagem após pausa
            else:
                time.sleep(retry_count * 5)


# ==============================================
# 3° BLOCO - Monitoramento
# ==============================================

def monitorar_radios():
    
    # 1° Ponto: Limpeza do diretório primário, como geramos diversos arquivos, alguns com erros inclusive, precisamos, sempre ao reiniciar o código, limpar o diretório com todos os .wav anexos.
     
    temp_dir = "C:\\Users\\MKTROBO01\\Desktop\\Streaming"
    if os.path.exists(temp_dir):
        for file_name in os.listdir(temp_dir):
            if file_name.endswith(".wav"):
                try:
                    os.remove(os.path.join(temp_dir, file_name))
                    print(f"[LOG] Arquivo {file_name} removido com sucesso.")
                except Exception as e:
                    print(f"[ERRO] Erro ao remover o arquivo {file_name}: {e}")

    recognizer = sr.Recognizer()
    queue_transcricao = queue.Queue()

    
    ### Chamamos o arquivo que relaciona os URL da Radio Brasil e 0800 das bases OPEC 
    
    radios_dinamicos, radios_stream, radios_clientes = carregar_0800_dinamico()
    
    
    if radios_dinamicos and radios_stream:
        active_radios = [name for name in radios_stream if name in radios_dinamicos]
        print(f"[LOG] Iniciando monitoramento para {len(radios_stream)} emissoras.")
        print(f"[LOG] {len(active_radios)} emissoras com 0800 configurado serão monitoradas.")
        print(f"[LOG] {len(radios_stream) - len(active_radios)} emissoras foram ignoradas por não terem 0800 no dicionário.")

        # Aqui definimos as Threads a serem aplicadas para cada emissora para TRANSCRIÇÃO, o intuito é realizarmos toda operação em pararelelo, simultâneamente 
        # Especificamos a função queue que será alimentada com cada bloco de captura de forma sequencial 
        # radios_dinamico são trazidas

        threads_transcricao = []
        for i in range(20):
            
            # especificamos a função 'transcrever_audio' que será usada para cada Thread
            # args=(queue_transcricao, recognizer, radios_dinamicos, 600): Passa os argumentos necessários para a função:
            # queue_transcricao: A fila de transcrição compartilhada entre threads.
            # recognizer: Instância do reconhecedor de fala.
            # radios_dinamicos: Dicionário com os números 0800 para cada emissora.
            # Define a thread como daemon, o que significa que ela será automaticamente finalizada quando o programa principal terminar.
            
            thread = Thread(target=transcrever_audio, args=(queue_transcricao, recognizer, radios_dinamicos, radios_clientes, 600))
            thread.daemon = True
            thread.start()
            threads_transcricao.append(thread)
            print(f"[LOG] Thread de transcrição {i+1} iniciada.")
        
        # Inicia threads de captura apenas para emissoras que possuem números 0800 a serem monitorados a partir de uma lista de armazenamento.
        
        threads_captura = []
        
        #Percorre cada emissora (radio_name) e sua URL de streaming (stream_url) no dicionário radios_stream.
        for radio_name, stream_url in radios_stream.items():
            
            # Verifica se a rádio possui um número 0800 no dicionário antes de iniciar a captura
            # Se a emissora possui um 0800 associado, inicia-se uma thread para captura do audio em si
            
            if radio_name in radios_dinamicos:
                thread = Thread(target=capturar_audio, args=(radio_name, stream_url, queue_transcricao))
                thread.daemon = True
                thread.start()
                threads_captura.append(thread)
                print(f"[LOG] Iniciada captura de áudio para a emissora: {radio_name}")
            else:
                print(f"[LOG] Emissora '{radio_name}' ignorada (sem número 0800 no dicionário).")

        if threads_captura:
            print(f"[LOG] Iniciadas {len(threads_captura)} threads de captura de áudio para emissoras com 0800.")
        else:
            print("[LOG] Nenhuma thread de captura iniciada. Não há emissoras com 0800 no dicionário.")

        # Garantir que todas as threads de captura finalizem
        for thread in threads_captura:
            thread.join()
            print("[LOG] Thread de captura finalizada.")
    else:
        print("[ERRO] Falha ao carregar os dados de 0800 ou streams. Monitoramento não iniciado.")

if __name__ == "__main__":
    monitorar_radios()
    
    
#### Principais Funções e Utilidades #### 

# log_message(message, level="INFO"): Registra mensagens de log no console e em arquivos, auxiliando no monitoramento do sistema.

# normalize_text(text): Normaliza o texto da transcrição, removendo espaços, hífens e convertendo para minúsculas, facilitando a detecção.

# similar(a, b): Calcula a similaridade entre duas strings, utilizada na detecção de números 0800 com pequenas variações.

# carregar_0800_dinamico(): Carrega as URLs de streaming e números 0800 das emissoras, preparando os dados para o monitoramento.

# capturar_audio(radio_name, url, queue_transcricao, ...): Captura blocos de áudio das emissoras e os coloca na fila para transcrição.

# transcrever_audio(queue_transcricao, recognizer, radios_dinamicos, window_time): Transcreve os blocos de áudio e aplica a detecção de palavras-chave.

# encontrar_numero(transcription, radios_dinamicos, radio_name): Detecta números 0800 ou palavras-chave na transcrição.

# registrar_deteccao(fone, emissora): Registra as detecções no banco de dados, evitando duplicações em intervalos curtos.

# monitorar_radios(): Função principal que coordena todo o processo, iniciando as threads e integrando as funções acima.

#### Como baixar o ffmpeg: https://www.youtube.com/watch?v=JR36oH35Fgg&ab_channel=Koolac

