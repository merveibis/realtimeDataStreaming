# **Real-Time Data Streaming**

Bu proje, uçtan uca bir veri mühendisliği hattı oluşturmaya yönelik kapsamlı bir kılavuz görevi görmektedir. Apache Airflow, Python, Apache Kafka, Apache Zookeeper, Apache Spark ve Cassandra'yı içeren güçlü bir teknoloji yığını kullanarak veri alımından işleme ve son olarak depolamaya kadar her aşamayı kapsar. Dağıtım ve ölçeklenebilirlik kolaylığı için her şey Docker kullanılarak kapsayıcıya alınır.

**Sistem Mimarisi**
![image](https://github.com/merveibis/realtimeDataStreaming/assets/46818283/4d9d1de2-745b-4b9b-8028-ad7d7853c60c)

Proje aşağıdaki bileşenlerle tasarlanmıştır:

*  Veri Kaynağı: [randomuser.me](http://randomuser.me/) API, boru hattımız için rastgele kullanıcı verisi oluşturmak için kullanılır.
*  Apache Airflow: Boruyu düzenleme ve alınan veriyi bir PostgreSQL veritabanına depolama sorumluluğundadır.
*  Apache Kafka ve Zookeeper: PostgreSQL'den işleme motoruna yayın verisi için kullanılır.
*  Control Center ve Schema Registry: Kafka akışlarının izlenmesine ve şema yönetimine yardımcı olur.
*  Apache Spark: Veri işleme işlemleri için master ve worker düğümleriyle birlikte kullanılır.
*  Cassandra: İşlenmiş verilerin depolanacağı yerdir.

Random olarak insan profili üreten bir API(Application Programming Interface→ Uygulama programlama arayüzü) ile verilerimiz üretilir. Sonrasında iş akışlarını tanımlamak, zamanlamak ve izlemek için kullanılan açık kaynaklı bir platform olan Apache Airflow ile yakaladığımız verilerimiz, structured veri tabanlı olan PostgreSQL ile kaydedilir. Apache ZooKeeper açık kaynaklı sunucu sayesinde, dağıtık sistem olarak geliştirilen projede koordinasyon sağlayarak; veri tabanından gelen verilerimizi kaybetmememiz için bir nevi queue(kuyruk) mantığına dayanan yüksek verimli veri iletişimini kolaylaştıran Apache Kafka yazılımı kullanılır. Dağıtık sistemde verimli ve kaliteli süreç izlememize yardımcı olacak Apache Spark yazılımı ise işlerimizi daha da kolaylaştırır. Son olarak ise real-time olarak veri akışını sağladığımız verilerimizi unstructured yapıya sahip Cassandra veri tabanı ile depolama gerçekleştirilir. 

**Neler Öğrenilecek**
*  Apache Airflow ile bir veri boru hattı kurma
*  Apache Kafka ile gerçek zamanlı veri akışı
*  Apache Zookeeper ile dağıtılmış senkronizasyon
*  Apache Spark ile veri işleme teknikleri
*  Cassandra ve PostgreSQL ile veri depolama çözümleri
*  Docker ile tüm veri mühendislik kurulumunun konteynerleştirilmesi

**Teknolojiler**
*  Apache Airflow
*  Python
*  Apache Kafka
*  Apache Zookeeper
*  Apache Spark
*  Cassandra
*  PostgreSQL
*  Docker

# Airflow ile API’den Data Almak 
Pycharm IDE’si kullanılarak geliştirilen proje için IDE’de yeni bir proje oluşturulur. Projeye dags adında bir klasör eklenir. Proje dosyasında terminal üzerinden, projede ihtiyaç olan Airflow kütüphaneleri $ pip install apache-airflow komutu ile indirilir. Dosyalar indirildikten sonra dags klasörünün içerisine kafka_stream adında bir python dosyası eklenir.

kafka_stream.py dosyasına gerekli olan kütüphaneler eklenir.
```python
import uuid
from datetime import datetime                                                         
```
* `from datetime import datetime` : Python programlama dilinde **`datetime`** modülünden **`datetime`** sınıfını içeri aktarmak için kullanılır. Tarih-zaman işlemleri gerçekleştirilmesine yarar.
* `from airflow import DAG` : Apache Airflow kütüphanesinde DAG (Directed Acyclic Graph - Yönlendirilmiş Asiklik Grafik) oluşturmak için kullanılır.
* `from airflow.operators.python import PythonOperator` : Apache Airflow kütüphanesinde PythonOperator'u DAG'larda kullanabilmek için gerekli olan PythonOperator sınıfını içeri aktarmak için kullanılır. Apache Airflow'da **`PythonOperator`**, Python işlevlerini DAG içinde çalıştırmak için kullanılan bir operatördür. Bu operatör sayesinde, Python işlevleri DAG'larda tanımlanabilir ve çalıştırılabilir.

Apache Airflow DAG’ı için varsayılan argümanları içeren bir Python sözlüğü tanımlanır.
```python
default_args = {
    'owner': 'merveibis',
    'start_date': datetime(2024, 2, 10, 13, 38)
}                                                   
```
DAG'ın sahibini (**`owner`**) ve başlangıç tarihini (**`start_date`**) belirtir.

**`datetime(year, month, day, hour, minute)`** ifadesi, **`datetime`** modülünden **`datetime`** sınıfını kullanarak tarih-zaman nesnesi oluşturur. Bu nesne, DAG'ın başlangıç tarihini belirlemek için kullanılır. Bir DAG'ı oluştururken **`default_args`** parametresine değer olarak geçirilebilir. Bu şekilde, belirli bir DAG'da kullanılan tüm işlemler bu varsayılan ayarlarla başlatılabilir.

Veri akışı oluşturulması için veri alma ve veri işleme işlemlerini gerçekleştirmek adına get_data adında bir fonksiyon oluşturulur.
```python
def get_data():
    import requests

    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    res = res['results'][0]

    return res                                              
```
Bu Python fonksiyonu, "randomuser.me" API'sini kullanarak rastgele bir kullanıcı verisi almak için HTTP GET isteği gönderir. Ardından, bu veriyi JSON formatına dönüştürür ve bu JSON yanıtının "results" alanından ilk öğeyi (ilk kullanıcıyı) seçer.

Apache Kafka veya Apache Airflow gibi araçlar aracılığıyla bir veri akışına yazılabilmesi için datanın yeniden formatlanması gerekir.
```python
def format_data(res):
    data = {}
    location = res['location']
    data['id'] = uuid.uuid4()
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data                                             
```
Bu Python fonksiyonu, get_data fonksiyonundan alınan kullanıcı verisini belirli bir formata dönüştürmek için kullanılır. Gelen veri içeriği üzerinde belirli bir yapı oluşturularak, bu yapıda temsil edilen veri seti geri döndürülür.

Veri akışını başlatmak ve yönetmek için de ayrı bir fonksiyon oluşturulur.
```python
def stream_data():
    import json

    res = get_data()
    res = format_data(res)

    # uuid.uuid4() nesnesini stringe dönüştür
    res['id'] = str(res['id'])

    # datetime nesnelerini stringe dönüştür
    res['dob'] = str(res['dob'])
    res['registered_date'] = str(res['registered_date'])

		# indent=3 parametresi, JSON çıktısının daha okunabilir olması için girintileme ekler
    print(json.dumps(res, indent=3))

stream_data()                                         
```
Bu Python fonksiyonu, veri akışını başlatmak için kullanılır. get_data ve format_data fonksiyonlarını kullanarak rastgele kullanıcı verisi alır ve bu veriyi belirli bir formata dönüştürür. Son olarak, dönüştürülmüş veriyi JSON formatına dönüştürerek konsola yazdırır. Oluşturulan veri, Apache Kafka'ya veya bir veritabanına yazılmak üzere bir veri akışı içine aktarılabilir.

# Sistem Mimarisi için Docker Compose
## Docker Compose
Docker Compose, Docker konteynerlerini çoklu konteyner uygulamaları olarak tanımlamak, çalıştırmak ve yönetmek için kullanılan bir araçtır. Docker Compose, bir YAML dosyası aracılığıyla uygulamanın yapılandırmasını tanımlamanıza ve ardından bu yapılandırmaya dayanarak birden çok Docker konteynerini tek bir komutla başlatmanıza, durdurmanıza ve yönetmenize olanak tanır.

Docker Compose ile, bir uygulamanın tüm bileşenlerini tek bir komutta başlatabilirsiniz. Örneğin, bir web uygulaması geliştirirken, uygulamanın backend ve frontend bileşenlerinin yanı sıra veritabanı, arabellek ve diğer servisler de gerekebilir. Docker Compose, bu bileşenleri tanımlamak ve bunların Docker konteynerlerinde nasıl çalıştırılacağını belirtmek için kullanılabilir.

Bir Docker Compose dosyası genellikle aşağıdaki bileşenleri içerir:

1. **Servis Tanımları:** Uygulamanın farklı bileşenleri için Docker konteynerlerinin tanımları. Örneğin, bir PostgreSQL veritabanı servisi, bir Flask uygulaması servisi, bir Nginx web sunucusu servisi vb.
2. **Ağ Ayarları:** Servisler arasında iletişim kurmak için kullanılacak ağ yapılandırması.
3. **Depolama Ayarları:** Servislerin kullanacağı veri depolama ve bağlı olan depolama birimleri.

Docker Compose, bu yapılandırmayı temel alarak, tüm servisleri bir araya getirir ve Docker komutlarını çalıştırarak uygulamanın tüm bileşenlerini başlatır veya durdurur. Bu, geliştirme, test ve dağıtım aşamalarında çoklu konteyner uygulamalarını yönetmek için oldukça kullanışlı bir araçtır.
## Apache Zookeeper
Apache Zookeeper, dağıtılmış sistemlerin koordinasyonunu ve yönetimini sağlayan bir açık kaynaklı bir merkezi hizmettir. Özellikle büyük ölçekli dağıtık sistemlerde kilit yönetimi, yapılandırma bilgilerinin depolanması, lider seçimi ve senkronizasyon gibi kritik işlevleri yerine getirir.

Zookeeper, bir dizi sunucu (genellikle tek bir sunucu arıza durumlarını tolere edecek şekilde yapılandırılmış) tarafından oluşturulan bir kümeye dağıtılır. Bu sunucular, verileri ve durum bilgilerini kümenin tüm üyeleri arasında güncellemek ve senkronize etmek için birlikte çalışır. Zookeeper, yüksek oranda güvenilir ve dayanıklıdır, bu da onu büyük ölçekli sistemlerde merkezi bir bileşen olarak kullanılmasını sağlar.

Zookeeper'ın temel özellikleri şunlardır:

1. **Yapılandırma Yönetimi:** Sistem yapılandırma bilgilerini depolamak ve yönetmek için kullanılabilir. Bu, örneğin, dağıtılmış sistemlerdeki servislerin yapılandırma bilgilerini depolamak için kullanılabilir.
2. **Kilit Yönetimi:** Dağıtılmış sistemlerde kilitlerin koordinasyonunu sağlar. Birden çok istemcinin eşzamanlı olarak erişmesini engeller ve veri bütünlüğünü korur.
3. **Lider Seçimi:** Dağıtılmış sistemlerde lider olarak görev yapacak bir sunucunun seçilmesini sağlar. Özellikle yüksek kullanılabilirlik gerektiren sistemlerde lider seçimi önemlidir.
4. **Senkronizasyon:** İstemciler arasında senkronizasyon sağlar. Örneğin, birden çok istemcinin aynı anda bir eylem gerçekleştirmesini engelleyebilir.
5. **Uygulama Durum Takibi:** İstemcilerin uygulama durumunu izlemek ve güncellemek için kullanılabilir.

Bu özellikler, Zookeeper'ın karmaşık dağıtılmış sistemlerin koordinasyonunu ve yönetimini kolaylaştıran birçok senaryoda kullanılmasını sağlar. Zookeeper, özellikle Apache Kafka gibi diğer açık kaynaklı projelerde temel bir bileşen olarak yaygın bir şekilde kullanılmaktadır.

##
Docker üzerinden projenin geliştirilmesi için projeye docker-compose.yml formatında dosya oluşturulur. Bu Docker Compose dosyası, Apache Kafka ekosistemini çalıştırmak için yapılandırılmış bir çoklu servis ortamını tanımlar. Bu ortam, Zookeeper, Kafka Broker, Schema Registry ve Control Center gibi bileşenleri içerir.

**Zookeeper Servisi (`zookeeper`)**:
- **`confluentinc/cp-zookeeper:7.4.0`** Docker imajını kullanarak Zookeeper servisi başlatılır.
- Zookeeper'ın bağlanacağı port **`2181`** olarak ayarlanır.
- Zookeeper servisi sağlığını kontrol etmek için bir sağlık kontrolü tanımlanır.
- **`confluent`** adlı bir ağda bulunur.
```python
version: '3'

services:
  zookeeper:
      image: confluentinc/cp-zookeeper:7.4.0
      hostname: zookeeper
      container_name: zookeeper
      ports:
        - "2181:2181"
      environment:
        ZOOKEEPER_CLIENT_PORT: 2181
        ZOOKEEPER_TICK_TIME: 2000
      healthcheck:
        test: ['CMD', 'bash', '-c', "echo 'ruok' | nc localhost 2181"]
        interval: 10s
        timeout: 5s
        retries: 5
      networks:
        - confluent                                       
```

**Kafka Broker Servisi (`broker`)**:
- **`confluentinc/cp-server:7.4.0`** Docker imajını kullanarak Kafka Broker servisi başlatılır.
- Broker'ın bağlanacağı portlar **`9092`** ve **`9101`** olarak ayarlanır.
- Broker'ın Zookeeper ile iletişim kuracağı adres **`zookeeper:2181`** olarak belirtilir.
- Broker'ın Schema Registry'ye erişim sağlayacağı adres **`http://schema-registry:8081`** olarak belirtilir.
- Broker servisi sağlığını kontrol etmek için bir sağlık kontrolü tanımlanır.
- **`confluent`** adlı bir ağda bulunur.
```python
  broker:
    image: confluentinc/cp-server:7.4.0
    hostname: broker
    container_name: broker
    depends_on:
      zookeeper:
        condition: service_healthy
    ports:
      - "9092:9092"
      - "9101:9101"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: 'zookeeper:2181'
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://broker:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_METRIC_REPORTERS: io.confluent.metrics.reporter.ConfluentMetricsReporter
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_CONFLUENT_LICENSE_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_CONFLUENT_BALANCER_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_JMX_PORT: 9101
      KAFKA_JMX_HOSTNAME: localhost
      KAFKA_CONFLUENT_SCHEMA_REGISTRY_URL: http://schema-registry:8081
      CONFLUENT_METRICS_REPORTER_BOOTSTRAP_SERVERS: broker:29092
      CONFLUENT_METRICS_REPORTER_TOPIC_REPLICAS: 1
      CONFLUENT_METRICS_ENABLE: 'false'
      CONFLUENT_SUPPORT_CUSTOMER_ID: 'anonymous'
    networks:
      - confluent
    healthcheck:
      test: [ "CMD", "bash", "-c", 'nc -z localhost 9092' ]
      interval: 10s
      timeout: 5s
      retries: 5                                      
```

**Schema Registry Servisi (`schema-registry`)**:
- **`confluentinc/cp-schema-registry:7.4.0`** Docker imajını kullanarak Schema Registry servisi başlatılır.
- Schema Registry'nin bağlanacağı port **`8081`** olarak ayarlanır.
- Schema Registry servisi sağlığını kontrol etmek için bir sağlık kontrolü tanımlanır.
- **`confluent`** adlı bir ağda bulunur.
```python
schema-registry:
    image: confluentinc/cp-schema-registry:7.4.0
    hostname: schema-registry
    container_name: schema-registry
    depends_on:
      broker:
        condition: service_healthy
    ports:
      - "8081:8081"
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: 'broker:29092'
      SCHEMA_REGISTRY_LISTENERS: http://0.0.0.0:8081
    networks:
      - confluent
    healthcheck:
      test: [ "CMD", "curl", "-f", "http://localhost:8081/" ]
      interval: 30s
      timeout: 10s
      retries: 5                                   
```

**Control Center Servisi (`control-center`)**:
- **`confluentinc/cp-enterprise-control-center:7.4.0`** Docker imajını kullanarak Control Center servisi başlatılır.
- Control Center'ın bağlanacağı port **`9021`** olarak ayarlanır.
- Control Center servisi sağlığını kontrol etmek için bir sağlık kontrolü tanımlanır.
- **`confluent`** adlı bir ağda bulunur.
```python
control-center:
    image: confluentinc/cp-enterprise-control-center:7.4.0
    hostname: control-center
    container_name: control-center
    depends_on:
      broker:
        condition: service_healthy
      schema-registry:
        condition: service_healthy
    ports:
      - "9021:9021"
    environment:
      CONTROL_CENTER_BOOTSTRAP_SERVERS: 'broker:29092'
      CONTROL_CENTER_SCHEMA_REGISTRY_URL: "http://schema-registry:8081"
      CONTROL_CENTER_REPLICATION_FACTOR: 1
      CONTROL_CENTER_INTERNAL_TOPICS_PARTITIONS: 1
      CONTROL_CENTER_MONITORING_INTERCEPTOR_TOPIC_PARTITIONS: 1
      CONFLUENT_METRICS_TOPIC_REPLICATION: 1
      CONFLIENT_METRICS_ENABLE: 'false'
      PORT: 9021
    networks:
      - confluent
    healthcheck:
      test: [ "CMD", "curl", "-f", "http://localhost:9021/health" ]
      interval: 30s
      timeout: 10s
      retries: 5                                  
```

**Ağlar (`networks`)**:
- Tüm servisler **`confluent`** adlı bir ağda bulunur. Bu, servislerin aynı ağda iletişim kurmasını sağlar.
Bu Docker Compose dosyasını kullanarak, Kafka ekosistemini hızlıca başlatabilir ve test edebilirsiniz. Komut satırında, Docker Compose dosyasının olduğu dizindeyken **`docker-compose up`** komutunu çalıştırarak bu ortamı başlatabilirsiniz.
```python
networks:
  confluent:                               
```

docker-compose.yml dosyasını konfigüre ettikten sonra terminal üzerinden Docker platformu üzerinden projeye ait container ile images’ların çalışmasını sağlamak için;
```bash
docker-compose up -d                              
```
komutu ile ortam başlatılır.

# Kafka'ya Veri Akışı
Zookeper ve Kafka’yı bağladığımızı kontrol edilmesi gerekmektedir. Aynı zamanda Schema Registery kullanarak da tutarlı veri akışı kontrol altında tutulur.
## Schema Registery
Schema Registry, dağıtılmış sistemlerde veri schema'larını yönetmek için kullanılan bir hizmettir. Özellikle Apache Kafka ekosistemi içinde kullanılan bir bileşendir.

Apache Kafka, mesaj tabanlı bir sistemdir ve farklı kaynaklardan gelen verileri işlerken, bu verilerin nasıl yapılandırıldığı veya hangi veri türlerini içerdiği önemlidir. Schema Registry, bu tür veri schema'larını bir veri akışında kullanılan mesajların yapısal olarak tutarlı olmasını sağlamak için kullanılır.

Schema Registry, veri schema'larının merkezi bir konumda saklanmasını ve yönetilmesini sağlar. Bu, aynı veri akışında farklı kaynaklardan gelen verilerin uyumluluğunu sağlar. Schema Registry ayrıca, veri schema'larının sürüm kontrolünü ve uyumluluk kontrollerini yönetir, böylece bir schema değişikliği yapıldığında, mevcut veri akışlarının uyumluluğunu korumak mümkün olur.

##
Control-Center ile veri akışı görselleştirilerek yönetim kolaylaştırılır.

Verileri alarak belli bir formatta elde edilmesine yarayan **`stream_data`** fonksiyonu Kafka ile bağlanarak verilerin dinlenmesi için güncellenir.
```python
def stream_data():
    import json
    from kafka import KafkaProducer
    import time

    res = get_data()
    res = format_data(res)

    # uuid.uuid4() nesnesini stringe dönüştür
    res['id'] = str(res['id'])

    # datetime nesnelerini stringe dönüştür
    res['dob'] = str(res['dob'])
    res['registered_date'] = str(res['registered_date'])

    # indent=3 parametresi, JSON çıktısının daha okunabilir olması için girintileme ekler
    print(json.dumps(res, indent=3))

    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], max_block_ms=5000)

    producer.send('users_created', json.dumps(res).encode('utf-8'))                             
```
Fonksiyona eklenen kısım;

Bir KafkaProducer oluşturur ve **`users_created`** adlı bir Kafka konusuna JSON formatında bir mesaj gönderir. Ancak, burada bir JSON objesini byte dizisine dönüştürmek için **`json.dumps(res).encode('utf-8')`** kullanılır.

Terminal üzerinden proje çalıştırıldığında;
```bash
python dags/kafka_stream.py                            
```
control-center üzerinden topics sekmesinde users_created adında bir topic’in oluşturulacaktır. Yeniden veri gönderimi yapıldığında da topics/messages sekmesinde mesajın dinlenildiği görülecektir. Bununla birlikte Kafka-Zookeper-Control Center-Schema Registery arasında kaliteli bir bağlantı kurulduğu anlaşılabilir.

Sıra Airflow üzerinden Kafka’ya veri pushlamaktır. Bunun için docker-compose.yml dosyasında web-server konfigürasyona eklenmelidir.
```python
webserver:
    image: apache/airflow:2.6.0-python3.9
    command: webserver
    entrypoint: ['/opt/airflow/script/entrypoint.sh']
    depends_on:
      - postgres
    environment:
      - LOAD_EX=n
      - EXECUTOR=Sequential
      - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
      - AIRFLOW_WEBSERVER_SECRET_KEY=this_is_a_very_secured_key
    logging:
      options:
        max-size: 10m
        max-file: "3"
    volumes:
      - ./dags:/opt/airflow/dags
      - ./script/entrypoint.sh:/opt/airflow/script/entrypoint.sh
      - ./requirements.txt:/opt/airflow/requirements.txt
    ports:
      - "8080:8080"
    healthcheck:
      test: ['CMD-SHELL', "[ -f /opt/airflow/airflow-webserver.pid ]"]
      interval: 30s
      timeout: 30s
      retries: 3
    networks:
      - confluent                         
```
**Web Server Servisi (`webserver`)**:

Apache Airflow web sunucusunu başlatır. Airflow'un kullanıcı arayüzüne erişim sağlar ve DAG'leri yönetmek için bir arayüz sunar.

Apache Airflow konteynerinin başlatılması sırasında bir dizi önemli görevi otomatikleştirmek için projeye script yazılır. Bunun için projenin klasör yapısında script adında klasör oluşturularak entrypoint.sh  adında dosya oluşturulur. 
```python
#!/bin/bash
set -e

if [ -e "/opt/airflow/requirements.txt" ]; then
  $(command python) pip install --upgrade pip
  $(command -v pip) install --user -r requirements.txt
fi

if [ ! -f "/opt/airflow/airflow.db" ]; then
  airflow db init && \
  airflow users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@example.com \
    --password admin
fi

$(command -v airflow) db upgrade

exec airflow webserver                         
```
Bu giriş betiği, Apache Airflow konteynerini başlatırken çalıştırılacak bir bash betiğidir. İşte betiğin adım adım açıklaması:

- **`set -e`**: Skriptin çalışması sırasında herhangi bir hata oluştuğunda (komut başarısız olduğunda) derhal durmayı sağlar.
- **`if [ -e "/opt/airflow/requirements.txt" ]; then ... fi`**: Konteynerde **`/opt/airflow/requirements.txt`** dosyasının var olup olmadığını kontrol eder. Eğer varsa, pip'i günceller ve gereksinimleri yükler.
- **`if [ ! -f "/opt/airflow/airflow.db" ]; then ... fi`**: Konteynerde **`/opt/airflow/airflow.db`** dosyasının var olup olmadığını kontrol eder. Eğer yoksa, Airflow veritabanını başlatır (**`airflow db init`**) ve bir yönetici kullanıcısı oluşturur.
- **`$(command -v airflow) db upgrade`**: Veritabanını yükseltmek için **`airflow db upgrade`** komutunu çalıştırır. Bu, veritabanını oluşturur veya günceller.
- **`exec airflow webserver`**: Son olarak, Airflow web sunucusunu başlatmak için **`airflow webserver`** komutunu çalıştırır. **`exec`** kullanarak bu komutu başlatır, böylece işlem sonlandığında konteyner de sonlanır.

Oluşturulan script, Airflow konteynerini başlatmak ve çalıştırmak için gereken tüm adımları otomatikleştirir ve hızlı bir şekilde bir Airflow ortamını hazır hale getirir. Ayrıca, yeni bir konteyner oluşturulduğunda veya ortamda değişiklik yapıldığında bu adımları tekrarlamak için manuel müdahaleye gerek kalmaz. Bu, ortamın sürekli ve güvenilir olmasını sağlar.

Airflow Scheduler'ı ve PostgreSQL sunucusunu başlatmak için gereken tüm adımları otomatikleştirmek adına docker-compose.yml dosyasında güncelleme yapılmalıdır.
```python
  scheduler:
    image: apache/airflow:2.6.0-python3.9
    depends_on:
      webserver:
        condition: service_healthy
    volumes:
      - ./dags:/opt/airflow/dags
      - ./script/entrypoint.sh:/opt/airflow/script/entrypoint.sh
      - ./requirements.txt:/opt/airflow/requirements.txt
    environment:
      - LOAD_EX=n
      - EXECUTOR=Sequential
      - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
      - AIRFLOW_WEBSERVER_SECRET_KEY=this_is_a_very_secured_key
    command: bash -c "pip install -r ./requirements.txt && airflow db upgrade && airflow scheduler"
    networks:
      - confluent

  postgres:
    image: postgres:14.0
    environment:
      - POSTGRES_USER=airflow
      - POSTGRES_PASSWORD=airflow
      - POSTGRES_DB=airflow
    logging:
      options:
        max-size: 10m
        max-file: "3"
    networks:
      - confluent                             
```

**Scheduler (`scheduler`)**: 
Airflow Scheduler'ını başlatır. Scheduler, DAG'leri ne zaman çalıştırılacağını planlar ve çalıştırır.
- **image**: Scheduler'ı çalıştırmak için kullanılacak Docker imajı belirtilir.
- **depends_on**: Scheduler'ın başlaması için gereken diğer servisleri belirtir. Bu durumda, Scheduler'ın başlamadan önce Web Sunucusu'nun sağlıklı olmasını bekler.
- **volumes**: Konteyner ve host arasında bağlantıyı sağlar. Burada, DAG'lerin, başlatma betiğinin ve gereksinimler dosyasının konteynere bağlanması sağlanmıştır.
- **environment**: Scheduler için ortam değişkenlerini belirtir. Örneğin, veritabanı bağlantısı, yürütücü türü, gizli anahtar vb.
- **command**: Scheduler'ın başlatılması için çalıştırılacak komutları belirtir. Bu durumda, gereksinimlerin yüklenmesi (**`pip install -r ./requirements.txt`**), veritabanının güncellenmesi (**`airflow db upgrade`**) ve Scheduler'ın başlatılması (**`airflow scheduler`**) sağlanır.
- **networks**: Scheduler'ın hangi ağda bulunacağını belirtir.

**PostgreSQL (`postgres`)**: 
PostgreSQL veritabanı sunucusunu başlatır. Airflow, çalışma zamanı durumunu ve veritabanı geçmişini saklamak için bir veritabanına ihtiyaç duyar.
- **image**: PostgreSQL sunucusunu başlatmak için kullanılacak Docker imajı belirtilir.
- **environment**: PostgreSQL sunucusu için ortam değişkenlerini belirtir. Kullanıcı adı, şifre, veritabanı adı gibi.
- **logging**: PostgreSQL günlük ayarlarını belirtir.
- **networks**: PostgreSQL sunucusunun hangi ağda bulunacağını belirtir.

> Bu sırada projeye requirements.txt dosyasının var olduğu kontrol edilmelidir.

Güncellemeler sonucunda terminalden docker komutu çalıştırılır. Docker üzerinden [localhost:8080](http://localhost:8080) portu üzerinden web-server ile airflow arayüzüne ulaşılır.

Airflow arayüzünde;

user:admin

password:admin 

ile giriş yapılır. Arayüzde scheduler ile ilgili uyarı ile karşılaşılacaktır. Yeniden docker komutu çalıştırıldığında scheduler starter status’üne gelir ve airflow arayüzünden uyarı gider.

Random kullanıcı üreten api’den direkt olarak Kafka’ya veri göndermek için kafka_stream.py dosyasındaki stream_data() fonksiyonunun çağrılması yerine stream_data() fonksiyonun içinde güncellemeler yapılır ve DAG oluşturulur.
```python
  with DAG(
    'user-automation',
    default_args=default_arg,
    schedule_interval='@daily',
    catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )                       
```
Oluşturulan DAG sayesinde otomatik data aktarımı için başlangıç adımı yapılmış olur. Airflow arayüzü yenilendiğinde ise oluşturulan DAG ismi → user_automation adında DAG sayfada görünmelidir.

stream_data() fonksiyonunun son hali;

```python
import json
    from kafka import KafkaProducer
    import time
    import logging

    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60: #1 minute
            break
        try:
            res = get_data()
            res = format_data(res)

            # uuid.uuid4() nesnesini stringe dönüştür
            res['id'] = str(res['id'])

            # datetime nesnelerini stringe dönüştür
            res['dob'] = str(res['dob'])
            res['registered_date'] = str(res['registered_date'])

            producer.send('users_created', json.dumps(res).encode('utf-8'))
        except Exception as e:
            logging.error(f'An error occured: {e}')
            continue                      
```

şeklinde olmalıdır.

- **KafkaProducer**: Kafka'ya mesajlar göndermek için bir **`KafkaProducer`** nesnesi oluşturur. **`bootstrap_servers`** parametresi, Kafka broker'ının adresini belirtir.
- **time.time()**: Şu anki zamanı alır.
- **while True**: Sonsuz bir döngü başlatır.
    - **if time.time() > curr_time + 60**: Eğer geçen süre 1 dakikayı (60 saniye) aşarsa döngüden çıkar. Bu, betiğin 1 dakika boyunca çalışmasını sağlar.
    - **try-except bloğu**: Mesaj gönderme işlemi döngü içinde gerçekleştirilir. **`get_data()`** ve **`format_data()`** fonksiyonları ile veri alınır ve biçimlendirilir. Ardından, mesaj Kafka'ya **`producer.send()`** ile gönderilir.
        - **`json.dumps(res).encode('utf-8')`**: Mesaj, JSON formatına dönüştürülür ve ardından UTF-8 formatında kodlanır. Bu, mesajın Kafka'ya iletilmesini sağlar.
        - Eğer bir hata oluşursa (**`Exception`**), **`logging.error()`** ile hata günlüğe kaydedilir ve döngü devam eder (**`continue`**).

Bu betik, Kafka'ya düzenli aralıklarla veri göndermek için kullanılabilir. Örneğin, bir veri kaynağından (API, veritabanı vb.) veri alınabilir, biçimlendirilebilir ve ardından Kafka'ya gönderilebilir. Bu, gerçek zamanlı veri akışı işleme senaryolarında yaygın olarak kullanılır.

Airflow ekranında **user-automation** DAG’i auto-refresh’lenirse Control-Center arayüzünde **users_created**’a ait mesajlarda verilerin aktarıldığı görülmelidir.

# Apache Spark ve Cassandra Kurulumu
Projeye Spark ve Cassandra veritabanını da dahil etmek için docker-compose.yml dosyasında bunlara ait konfigürasyonlar eklenmelidir.

```python
  spark-master:
    image: bitnami/spark:latest
    command: bin/spark-class org.apache.spark.deploy.master.Master
    ports:
      - "9090:8080"
      - "7077:7077"
    networks:
      - confluent
      
  spark-worker:
    image: bitnami/spark:latest
    command: bin/spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077
    depends_on:
      - spark-master
    environment:
      SPARK_MODE: worker
      SPARK_WORKER_CORES: 2
      SPARK_WORKER_MEMORY: 1g
      SPARK_MASTER_URL: spark://spark-master:7077
    networks:
      - confluent

  cassandra_db:
    image: cassandra:latest
    container_name: cassandra
    hostname: cassandra
    ports:
      - "9042:9042"
    environment:
      - MAX_HEAP_SIZE=512M
      - HEAP_NEWSIZE=100M
      - CASSANDRA_USERNAME=cassandra
      - CASSANDRA_PASSWORD=cassandra
    networks:
      - confluent
```

**Spark Servisi (`spark-master && spark-worker`)**:

- **spark-master**: Apache Spark'ın bir ana düğümünü başlatır. Ana düğüm, kümenin yönetimini sağlar.
    - **image**: Apache Spark'ı içeren Docker imajı belirtilir.
    - **command**: Spark ana düğümünü başlatmak için komut belirtilir.
    - **ports**: Spark web arabirimine ve Spark bağlantı noktasına giden portlar belirtilir.
    - **networks**: Hangi ağda bulunacağını belirtir.
- **spark-worker**: Apache Spark'ın bir işçi düğümünü başlatır. İşçi düğümleri, ana düğümden iş yükü alır ve işleri gerçekleştirir.
    - **image**: Apache Spark'ı içeren Docker imajı belirtilir.
    - **command**: Spark işçi düğümünü başlatmak için komut belirtilir.
    - **depends_on**: İşçi düğümünün başlaması için gereken diğer servisler belirtilir.
    - **environment**: İşçi düğümü için ortam değişkenleri belirtilir.
    - **networks**: Hangi ağda bulunacağını belirtir.

**Cassandra Servisi (`cassandra_db`)**:

- **cassandra_db**: Apache Cassandra veritabanını başlatır.
    - **image**: Cassandra veritabanını içeren Docker imajı belirtilir.
    - **container_name, hostname, ports, environment**: Cassandra için konteyner yapılandırması belirtilir.
    - **networks**: Hangi ağda bulunacağını belirtir.

Python projesi ile Cassandra veritabanına erişmek ve Cassandra verileri üzerinde işlemler yapmak için;

```python
pip install cassandra-driver
```

komutu kullanılır. Komut çalıştırıldıktan sonra, Python ortamında **`cassandra-driver`** adlı bir kütüphane mevcut olur ve bu kütüphaneyi kullanarak Cassandra veritabanına bağlanabilir ve sorgular gerçekleştirilebilir. Bu sürücü, Cassandra'nın tüm temel işlevselliğini destekler ve veritabanı ile etkileşimde bulunmak için gerekli olan tüm araçları sağlar.

Python projesi ile Apache Spark kümesine bağlanmak ve Spark işlemleri gerçekleştirmek için;

```python
pip install spark pyspark
```

komutu kullanılır. Komut çalıştırıldıktan sonra, Python ortamında **`pyspark`** adlı bir kütüphane mevcut olur ve bu kütüphane aracılığıyla Apache Spark'ı kullanabilir. Bu, Spark'ı veri işleme ve analiz işlemlerinde Python programları ile birlikte kullanmanıza olanak tanır.

# Cassandra'ya Veri Akışı
Veri akışı için spark_stream.py dosyası üzerinde gerekli olacak kütüphaneler veya fonksiyonlar import edilir.

```python
import logging

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
```

Projenin doğrudan çalıştırılmasını kontrol etmek için;

```python
if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()
```

dosyada main fonksiyonu oluşturulur. Daha sonrasında Apache Spark'a bir bağlantı oluşturulması için **`create_spark_connection()`** 

oluşturulur . Bu fonksiyon Spark'a bağlanırken, Cassandra bağlantısını da yapılandırır. 

```python
def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn
```

- **`SparkSession.builder`**: Bir SparkSession oluşturmak için bir builder nesnesi alır.
- **`.appName('SparkDataStreaming')`**: Spark uygulamasının adını belirler. Bu, Spark Web UI ve log dosyalarında görüntülenecek.
- **`.config('spark.jars.packages', ...)`**: Gerekli bağımlılıkları sağlamak için Spark bağımlılıklarını yapılandırır. Bu durumda, Cassandra Connector ve Kafka paketlerini ekler.
- **`.config('spark.cassandra.connection.host', 'localhost')`**: Cassandra veritabanına bağlanmak için kullanılacak ana bilgisayar adresini belirtir.
- **`.getOrCreate()`**: Varsa mevcut bir SparkSession döndürür, yoksa yeni bir tane oluşturur.

Eğer SparkSession oluşturulursa, bağlantının başarılı olduğunu ve bir SparkContext'in **`ERROR`** seviyesine ayarlandığını loglar. Eğer oluşturulamazsa, bir hata mesajı kaydedilir ve **`None`** döndürülür.

Cassandra veri tabanına bir bağlantı oluşturmak ve Cassandra kümesine bağlanarak ve bir oturum başlatmak için;

```python
def create_cassandra_connection():
    try:
        # connecting to the cassandra cluster
        cluster = Cluster(['localhost'])

        cas_session = cluster.connect()

        return cas_session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
        return None
```

fonksiyonu oluşturulur. 

- **`Cluster(['localhost'])`**: Bağlanılacak Cassandra kümesini belirtir. Bu örnekte, sadece yerel makinede çalışan bir Cassandra örneğine bağlanılır.
- **`cluster.connect()`**: Cluster nesnesinden bir Cassandra Session nesnesi oluşturur. Bu Session nesnesi, Cassandra veritabanına sorguları yürütmek ve veri alışverişi yapmak için kullanılır.

Bağlantı başarılıysa, Cassandra Session nesnesi döndürülür. Aksi takdirde, bir hata mesajı kaydedilir ve **`None`** döndürülür. Eğer spark bağlantısı sağlanırsa cassandra bağlantısı kurulur. Bunun kontrolü için main() fonksiyona ekleme yapılır.

```python
if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()

		**if spark_conn is not None:
		        session = create_cassandra_connection()**
```

Eğer cassandra oturumu da başarılı ile sağlandıysa;

```python
if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()

		if spark_conn is not None:
		   session = create_cassandra_connection()

				**if session is not None:
            create_keyspace(session)
            create_table(session)**
```

create_keyspace(session) ve create_table(session) fonksiyonları çağırılır.

**`create_keyspace(session)`**: Bu fonksiyon, Cassandra bağlantısı üzerinden bir anahtar alan oluşturur. Anahtar alan, Cassandra'da verilerin depolandığı temel bir yapıdır. Eğer anahtar alan zaten varsa, bu işlev anahtar alanı tekrar oluşturmaz.

```python
def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")
```

- **`session.execute(query)`**: Cassandra sorgusunu yürütür. Bu durumda, **`CREATE KEYSPACE`** ifadesi verilen keyspace'i oluşturur. **`IF NOT EXISTS`** ifadesi, keyspace'in zaten var olması durumunda tekrar oluşturulmamasını sağlar.
- Oluşturulan keyspace'in adı **`spark_streams`** olarak belirlenmiştir.
- Keyspace, **`'SimpleStrategy'`** replikasyon stratejisi ile oluşturulmuştur. Bu strateji, birincil veri merkezinde tek bir replika kullanır (**`replication_factor: '1'`**).
- Fonksiyon, keyspace başarıyla oluşturulduğunda bir bildirim mesajı yazdırır. Bu, keyspace oluşturmanın tamamlandığını gösterir.

**`create_table(session)`**: Bu fonksiyon, Cassandra bağlantısı üzerinden bir tablo oluşturur. Bu tablo, verilerin belirli bir şekilde yapılandırılarak depolandığı bir veri tabanı nesnesidir. **`spark_streams`** anahtar alanında **`created_users`** adında bir tablo oluşturur. Eğer tablo zaten varsa, bu işlev tabloyu tekrar oluşturmaz.

```python
def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.created_users (
        id UUID PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        address TEXT,
        post_code TEXT,
        email TEXT,
        username TEXT,
        registered_date TEXT,
        phone TEXT,
        picture TEXT);
    """)

    print("Table created successfully!")
```

- **`session.execute(query)`**: Cassandra sorgusunu yürütür. Bu durumda, **`CREATE TABLE`** ifadesi verilen tabloyu oluşturur. **`IF NOT EXISTS`** ifadesi, tablonun zaten var olması durumunda tekrar oluşturulmamasını sağlar.
- Oluşturulan tablonun adı **`created_users`** olarak belirlenmiştir ve bu tablo **`spark_streams`** keyspace'inde yer alır.
- Tablonun sütunları aşağıdaki gibidir:
    - **`id`**: UUID türünde birincil anahtar (primary key).
    - **`first_name`**, **`last_name`**, **`gender`**, **`address`**, **`post_code`**, **`email`**, **`username`**, **`registered_date`**, **`phone`**, **`picture`**: TEXT türünde sütunlar.
- Fonksiyon, tablonun başarıyla oluşturulduğuna dair bir bildirim mesajı yazdırır. Bu, tablo oluşturmanın tamamlandığını gösterir.

Cassandra bağlantısı üzerinden veri eklemeyi sağlamak için insert_data() fonksiyonu oluşturulur.

İçine bir dizi argüman alır: **`user_id`**, **`first_name`**, **`last_name`**, **`gender`**, **`address`**, **`postcode`**, **`email`**, **`username`**, **`dob`**, **`registered_date`**, **`phone`** ve **`picture`**. Bu verileri kullanarak, Cassandra tablosuna bir satır ekler.

```python
def insert_data(session, **kwargs):
    print("inserting data...")

    user_id = kwargs.get('id')
    first_name = kwargs.get('first_name')
    last_name = kwargs.get('last_name')
    gender = kwargs.get('gender')
    address = kwargs.get('address')
    postcode = kwargs.get('post_code')
    email = kwargs.get('email')
    username = kwargs.get('username')
    dob = kwargs.get('dob')
    registered_date = kwargs.get('registered_date')
    phone = kwargs.get('phone')
    picture = kwargs.get('picture')

    try:
        session.execute("""
            INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address, 
                post_code, email, username, dob, registered_date, phone, picture)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (user_id, first_name, last_name, gender, address,
              postcode, email, username, dob, registered_date, phone, picture))
        logging.info(f"Data inserted for {first_name} {last_name}")

    except Exception as e:
        logging.error(f'could not insert data due to {e}')
```

- **`session.execute(query, params)`**: Belirtilen sorguyu Cassandra'da yürütür. Bu durumda, **`INSERT INTO`** ifadesi verilen tabloya yeni bir satır ekler.
- SQL sorgusunda, **`%s`** placeholder'ları yerine sırasıyla **`VALUES`** kısmında verilen parametreler geçirilir. Bu parametreler, fonksiyon çağrısı sırasında **kwargs (anahtar kelime argümanları) olarak verilir.
- **`kwargs.get('field_name')`**: Verilen alan adına sahip anahtarın değerini döndürür. Eğer verilen anahtar yoksa **`None`** döner.
- Parametreler, Cassandra tablosunun ilgili sütunlarına eşlenir. Bu şekilde, her bir kullanıcı için bir satır eklenir.
- İşlem başarılı olursa, bir log mesajı yazdırılır. Aksi takdirde, bir hata mesajı kaydedilir.

Bu fonksiyon, genellikle Apache Kafka üzerinden alınan verileri işleyerek bunları Cassandra gibi veri depolama sistemlerine kaydetmek için kullanılır.

Kafka'ya bir Spark DataFrame bağlantısı oluşturmak için connect_to_kafka(spark_conn) fonksiyonu oluşturulur. Kafka'nın **`users_created`** konusunu izler ve bu konudan gelen verileri alır.

```python
def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df
```

- **`spark_conn.readStream.format('kafka')`**: Kafka kaynak verisi için bir okuma akışı oluşturur.
- **`option('kafka.bootstrap.servers', 'localhost:9092')`**: Kafka sunucusuna bağlanmak için gerekli olan bootstrap sunucusunu belirtir.
- **`option('subscribe', 'users_created')`**: Kafka konusu **`users_created`**'e abone olur. Bu, Kafka'daki belirli bir konudan veri alınacağı anlamına gelir.
- **`option('startingOffsets', 'earliest')`**: Kafka'nın en erken (ilk) mesajdan başlayarak veri sağlamasını sağlar.
- **`load()`**: Belirtilen ayarlarla Kafka verilerini yükler ve bir DataFrame döndürür.

Fonksiyon, başarılı olursa bir bilgi mesajı yazdırır ve oluşturulan Spark DataFrame'ini döndürür. Başarısız olursa bir uyarı mesajı yazdırır ve **`None`** değerini döndürür.

Kafka'dan gelen JSON verilerini işlemek için bir Spark DataFrame oluşturur. Bu DataFrame, JSON verilerini belirli bir yapıya dönüştürür ve işlenebilir bir formata getirir. Bunun için;

```python
def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(sel)

    return sel
```

fonksiyonu oluşturulur. 

- **`schema`**: DataFrame'in yapısını tanımlayan bir **`StructType`** nesnesi oluşturur. Her bir **`StructField`**, DataFrame'deki bir sütunun adını, veri tipini ve nullable olup olmadığını belirtir.
- **`spark_df.selectExpr("CAST(value AS STRING)")`**: Kafka'dan gelen verilerin **`value`** sütununu stringe dönüştürür. Çoğu zaman Kafka mesajları JSON formatında gelir, bu nedenle **`value`** sütunu genellikle bir JSON dizesini içerir.
- **`from_json(col('value'), schema)`**: JSON formatındaki verileri belirtilen şemaya (**`schema`**) göre ayrıştırır. Bu, JSON verilerini DataFrame sütunlarına dönüştürür.
- **`.alias('data')`**: Oluşturulan DataFrame sütunlarının adını **`data`** olarak değiştirir.
- **`.select("data.*")`**: **`data`** adlı sütunları seçer ve yeni bir DataFrame oluşturur. Bu, her bir JSON nesnesini DataFrame sütunlarına dönüştürür.

Son olarak oluşturulan fonksiyonları, bağlantıları kontrol etmek için main fonksiyonu üzerinde güncellemeler yapılır.

```python
if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # connect to kafka with spark connection
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)

            logging.info("Streaming is being started...")

            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                               .option('checkpointLocation', '/tmp/checkpoint')
                               .option('keyspace', 'spark_streams')
                               .option('table', 'created_users')
                               .start())

            streaming_query.awaitTermination()
```

Bu kod, veri akışını Kafka'dan alır, belirli bir yapıya göre işler ve sonuçları Cassandra'ya yazan bir Spark Streaming uygulamasını başlatır. Aşağıdaki adımları gerçekleştirir:

1. **`create_spark_connection()`**: Spark Session oluşturur. Bu, Spark uygulamalarını başlatmak ve Spark ile etkileşimde bulunmak için gereklidir.
2. **`connect_to_kafka(spark_conn)`**: Kafka'ya bağlanır ve gelen verileri bir DataFrame'e yükler.
3. **`create_selection_df_from_kafka(spark_df)`**: Kafka'dan gelen JSON verilerini belirli bir şema yapısına göre seçilen bir DataFrame'e dönüştürür.
4. **`create_cassandra_connection()`**: Cassandra veritabanına bağlanır.
5. **`create_keyspace(session)`**: Cassandra'da bir keyspace oluşturur.
6. **`create_table(session)`**: Oluşturulan keyspace içinde bir tabl o oluşturur.
7. Kafka'dan gelen verileri Cassandra tablosuna yazmak için bir Spark Streaming işlemi başlatır. Bu işlem, **`writeStream`** kullanarak Cassandra'ya yazma işlemini gerçekleştirir. Bu işlem, belirli aralıklarla gelen verileri Cassandra'ya yazmaya devam eder.
8. **`streaming_query.awaitTermination()`**: Akış işlemi, işlemin sonlandırılana kadar bekler. Bu, Spark Streaming işleminin sürekli olarak çalışmasını sağlar.

Bu kod bloğu, Kafka'dan gelen verileri işlemek için Spark ve Cassandra'yı kullanarak bir veri akışı uygulaması başlatır ve verileri kaydeder.

Özetle;

1. **Spark Bağlantısı Oluşturma (`create_spark_connection()`)**
2. **Kafka'ya Bağlanma ve Veri Okuma (`connect_to_kafka()`)**
3. **Kafka DataFrame'ini İşleme (`create_selection_df_from_kafka()`)**
4. **Cassandra Bağlantısı Oluşturma (`create_cassandra_connection()`)**
5. **Keyspace Oluşturma (`create_keyspace(session)`)**
6. **Tablo Oluşturma (`create_table(session)`)**
7. **Veri Ekleme (`insert_data(session, **kwargs)`)**
8. **Spark Streaming Başlatma (`if __name__ == "__main__":`)**

Veri akışını başlatmak için terminal üzerinden;

```python
python spark_streams.py
```

komutu çalıştırılır. Bu sırada docker üzerinden bütün images’ların sağlıklı bir şekilde çalıştığından emin olunmalıdır.


# _Merve İbiş_
























