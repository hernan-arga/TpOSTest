#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<commons/config.h>
#include<unistd.h>
#include<sys/socket.h>
#include<sys/types.h>
#include<netinet/in.h>
#include<arpa/inet.h>
#include<pthread.h>
#include<commons/log.h>
#include<commons/collections/list.h>
#include<commons/temporal.h>

typedef enum {
	SELECT, INSERT, CREATE, DESCRIBE, DROP, OPERACIONINVALIDA
} OPERACION;

typedef struct{
	int32_t PUERTO;
	char* IP_FS;
	int32_t PUERTO_FS;
	int32_t RETARDO_MEM;
	int32_t RETARDO_FS;
	int32_t TAM_MEM;
	int32_t RETARDO_JOURNAL;
	int32_t RETARDO_GOSSIPING;
	int32_t MEMORY_NUMBER;
	char** IP_SEEDS;
	char** PUERTO_SEEDS;
}archivoConfiguracion;

typedef struct{
	uint32_t NUMERO_PAGINA;
	uint32_t TIMESTAMP;
	int KEY;
	char* VALUE;
	bool MODIFICADO;
	uint32_t EN_USO;
}pagina;

typedef struct{
	char* PATH;
	char* TABLA_ASOCIADA;
	uint32_t NUMERO_SEGMENTO;
	//uint32_t CANTIDAD_PAGINAS;
	pagina PAGINAS[50];
	uint32_t EN_USO;
}segmento;

struct sockaddr_in serverAddress;
struct sockaddr_in serverAddressFS;
archivoConfiguracion t_archivoConfiguracion;
t_config *config;
int32_t server;
int32_t clienteKernel;
int32_t clienteFS;
uint32_t tamanoDireccion;
int32_t activado = 1;
pthread_t threadKernel;
pthread_t threadFS;
uint32_t tamanoValue;
bool sem1 = false;
bool sem2 = true;
segmento tablaSegmentos[50];
segmento segmentoNulo;
pagina paginaNulo;

segmento buscarSegmentoSegunTabla(char* tabla);
pagina buscarPagina(char* key, segmento segmentoAsociado);
pagina encontrarPaginaLibre(pagina tablaPaginas[]);
void serServidor();
void consola();
void controlarKernel();
OPERACION tipoDePeticion(char* peticion);
pagina ejecutarLRU(pagina tablaPaginas[]);
segmento buscarSegmentoLibre();
char* realizarSelect(char* tabla, char* key);
void conectarseAFS();
void conectarseAKernel();
void analizarInstruccion(char* instruccion);
void realizarComando(char** comando);
int insertarEnPaginaLibre(segmento segmentoAsociado, char* key, char* value);
void ejecutarJournaling();
void realizarInsert(char* tabla, char* key, char* value);
void realizarCreate(char* tabla, char* tipoConsistencia, char* numeroParticiones, char* tiempoCompactacion);

int main(int argc, char *argv[])
{
	config = config_create(argv[1]);

	t_archivoConfiguracion.PUERTO = config_get_int_value(config, "PUERTO");
	t_archivoConfiguracion.PUERTO_FS = config_get_int_value(config, "PUERTO_FS");
	t_archivoConfiguracion.IP_SEEDS= config_get_array_value(config, "IP_SEEDS");
	t_archivoConfiguracion.PUERTO_SEEDS = config_get_array_value(config, "PUERTO_SEEDS");
	t_archivoConfiguracion.RETARDO_MEM = config_get_int_value(config, "RETARDO_MEM");
	t_archivoConfiguracion.RETARDO_FS = config_get_int_value(config, "RETARDO_FS");
	t_archivoConfiguracion.TAM_MEM = config_get_int_value(config, "TAM_MEM");
	t_archivoConfiguracion.RETARDO_JOURNAL = config_get_int_value(config, "RETARDO_JOURNAL");
	t_archivoConfiguracion.RETARDO_GOSSIPING = config_get_int_value(config, "RETARDO_GOSSIPING");
	t_archivoConfiguracion.MEMORY_NUMBER = config_get_int_value(config, "MEMORY_NUMBER");

	void inicializarSegmentos();

	pthread_t threadSerServidor;
	int32_t idThreadSerServidor = pthread_create(&threadSerServidor, NULL, serServidor, NULL);

	pthread_t threadConsola;
	int32_t idthreadConsola = pthread_create(&threadConsola, NULL, consola, NULL);

	pthread_join(threadConsola, NULL);
	pthread_join(threadSerServidor, NULL);
}

void inicializarSegmentos()
{
	for(int i = 0; i < 50; i++)
	{
		tablaSegmentos[i].NUMERO_SEGMENTO = i;
		tablaSegmentos[i].EN_USO = 0;
		for(int j = 0; j < 50 ; j++)
		{
			tablaSegmentos[i].PAGINAS[j].NUMERO_PAGINA = j;
			tablaSegmentos[i].PAGINAS[j].EN_USO = 0;
		}
	}
	segmentoNulo.EN_USO = -1;
	paginaNulo.EN_USO = -1;
}

void serServidor()
{
	serverAddress.sin_family = AF_INET;
	serverAddress.sin_addr.s_addr = INADDR_ANY;
	serverAddress.sin_port = htons(t_archivoConfiguracion.PUERTO);

	server = socket(AF_INET, SOCK_STREAM, 0);

	setsockopt(server, SOL_SOCKET, SO_REUSEADDR, &activado, sizeof(activado));

	if(bind(server, (void*) &serverAddress, sizeof(serverAddress)) != 0)
	{
		perror("Fallo el bind");
	}

	printf( "Estoy escuchando\n");
	listen(server, 100);

	conectarseAKernel();
	conectarseAFS();
}

void conectarseAKernel()
{
	clienteKernel = accept(server, (void*) &serverAddress, &tamanoDireccion);
	printf( "Recibi una conexion en %d\n", clienteKernel);

	pthread_create(&threadKernel, NULL, (void*) controlarKernel, NULL);
	pthread_join(threadKernel, NULL);
}

void controlarKernel()
{
	while(1)
	{

	}

}

void conectarseAFS()
{
	clienteFS = socket(AF_INET, SOCK_STREAM, 0);
	serverAddressFS.sin_family = AF_INET;
	serverAddressFS.sin_port = htons(t_archivoConfiguracion.PUERTO_FS);
	serverAddressFS.sin_addr.s_addr = atoi(t_archivoConfiguracion.IP_FS);

	if (connect(clienteFS, (struct sockaddr *) &serverAddressFS, sizeof(serverAddressFS)) == -1)
	{
		perror("Hubo un error en la conexion \n");
	}

	recv(clienteFS, &tamanoValue, sizeof(tamanoValue), 0);

	sem1 = true;
}

void consola()
{
	while(!sem1)
	{

	}
	while(1)
	{
		char* instruccion = malloc(1000);
		do {
			fgets(instruccion, 1000, stdin);
		} while (!strcmp(instruccion, "\n"));
		analizarInstruccion(instruccion);
		free(instruccion);
	}
}

void analizarInstruccion(char* instruccion)
{
	char** comando = malloc(strlen(instruccion) + 1 );
	comando = string_split(instruccion, " \n");
	realizarComando(comando);
	free(comando);
}

void realizarComando(char** comando)
{
	while(!sem2){

	}
	char *peticion = comando[0];
	OPERACION accion = tipoDePeticion(peticion);
	char* tabla;
	char* key;
	char* value;
	switch(accion)
	{
		case SELECT:
			printf( "SELECT");
			tabla = comando[1];
			key = comando[2];
			realizarSelect(tabla, key);
			break;

		case INSERT:
			printf( "INSERT");
			tabla = comando[1];
			key = comando[2];
			value = comando[3];
			realizarInsert(tabla, key, value);
			break;

		case CREATE:
			printf("CREATE");
			char* tabla = comando[1];
			char* tipoConsistencia = comando[2];
			char* numeroParticiones = comando[3];
			char* tiempoCompactacion = comando[4];
			realizarCreate(tabla, tipoConsistencia, numeroParticiones, tiempoCompactacion);
			break;

		case OPERACIONINVALIDA:
			printf("OPERACION INVALIDA");
			break;
	}
}

OPERACION tipoDePeticion(char* peticion) {
	if (!strcmp(peticion, "SELECT"))
	{
		free(peticion);
		return SELECT;
	} else {
		if (!strcmp(peticion, "INSERT")) {
			free(peticion);
			return INSERT;
		} else {
			if (!strcmp(peticion, "CREATE")) {
				free(peticion);
				return CREATE;
			} else {
				free(peticion);
				return OPERACIONINVALIDA;
			}
		}
	}
}

char* realizarSelect(char* tabla, char* key)
{
	segmento segmentoAsociado = buscarSegmentoSegunTabla(tabla);
	if(segmentoAsociado.EN_USO != -1)
	{
		pagina paginaAsociada = buscarPagina(key, segmentoAsociado);
		if(paginaAsociada.EN_USO != -1)
		{
			printf(paginaAsociada.KEY);
			return paginaAsociada.KEY;
		}
		else
		{
			char* peticionValue;
			send(clienteFS, peticionValue, strlen(peticionValue), 0);
			char* value = malloc(tamanoValue);
			recv(clienteFS, &value, sizeof(value), 0);
			int num = insertarEnPaginaLibre(segmentoAsociado, key, value);
			printf(value);
			free(value);
			return segmentoAsociado.PAGINAS[num].VALUE;

		}
	}
	else
	{
		char* peticionValue = malloc(sizeof(int) + sizeof(int) + sizeof(int) + strlen(tabla) + strlen(key));
		strcpy(peticionValue, 1);
		strcat(peticionValue, strlen(tabla));
		strcat(peticionValue, tabla);
		strcat(peticionValue, strlen(key));
		strcat(peticionValue, key);
		send(clienteFS, peticionValue, strlen(peticionValue), 0);
		free(peticionValue);

		char* value = malloc(tamanoValue);
		recv(clienteFS, &value, sizeof(value), 0);

		segmentoAsociado = buscarSegmentoLibre();
		segmentoAsociado.PAGINAS[0].EN_USO = 1;
		segmentoAsociado.PAGINAS[0].KEY = key;
		segmentoAsociado.PAGINAS[0].MODIFICADO = 0;
		segmentoAsociado.PAGINAS[0].TIMESTAMP = temporal_get_string_time();
		segmentoAsociado.PAGINAS[0].VALUE = value;

		free(value);

		printf(segmentoAsociado.PAGINAS[0].VALUE);
		return segmentoAsociado.PAGINAS[0].VALUE;
	}
}

segmento buscarSegmentoLibre()
{
	for(int i = 0; i < 50; i++)
	{
		if(tablaSegmentos[i].EN_USO == 0)
			return tablaSegmentos[i];
	}
}

segmento buscarSegmentoSegunTabla(char* tabla)
{
	for(int i = 0; i < 50; i++)
	{
		segmento segmento = tablaSegmentos[i];
		if(segmento.EN_USO)
		{
			if(tablaSegmentos[i].TABLA_ASOCIADA == tabla)
				return tablaSegmentos[i];
		}
	}
	return segmentoNulo;
}

pagina buscarPagina(char* key, segmento segmentoAsociado)
{
	for(int i = 0; i < 50; i++)
	{
		if(segmentoAsociado.PAGINAS[i].EN_USO)
		{
			if(segmentoAsociado.PAGINAS[i].KEY == atoi(key))
			{
				return segmentoAsociado.PAGINAS[i];
			}
		}
	}
	return paginaNulo;
}

int insertarEnPaginaLibre(segmento segmentoAsociado, char* key, char* value)
{
	pagina primeraLibre = encontrarPaginaLibre(segmentoAsociado.PAGINAS);
	segmentoAsociado.PAGINAS[primeraLibre.NUMERO_PAGINA].EN_USO = 1;
	segmentoAsociado.PAGINAS[primeraLibre.NUMERO_PAGINA].KEY = atoi(key);
	segmentoAsociado.PAGINAS[primeraLibre.NUMERO_PAGINA].MODIFICADO = 0;
	segmentoAsociado.PAGINAS[primeraLibre.NUMERO_PAGINA].VALUE = value;
	segmentoAsociado.PAGINAS[primeraLibre.NUMERO_PAGINA].TIMESTAMP = atoi(temporal_get_string_time());
	return primeraLibre.NUMERO_PAGINA;

}

pagina encontrarPaginaLibre(pagina tablaPaginas[])
{
	for(int i = 0; i < 50; i++)
	{
		if(tablaPaginas[i].EN_USO == 0)
			return tablaPaginas[i];
	}
	return ejecutarLRU(tablaPaginas);
}

pagina ejecutarLRU(pagina tablaPaginas[])
{
	pagina candidataRemplazo = tablaPaginas[0];
	uint32_t tiempoMenor = candidataRemplazo.TIMESTAMP;

	for(int i = 1; i < 50; i++)
	{
		if(tablaPaginas[i].TIMESTAMP < tiempoMenor && !tablaPaginas[i].MODIFICADO)
		{
			candidataRemplazo = tablaPaginas[i];
			tiempoMenor = candidataRemplazo.TIMESTAMP;
		}
	}
	if(!candidataRemplazo.MODIFICADO)
		return candidataRemplazo;

	ejecutarJournaling();
	return candidataRemplazo;
	ejecutarLRU(tablaPaginas);
}

void ejecutarJournaling()
{
	sem2 = false;
	for(int i = 0; i < 50; i++)
	{
		for(int j = 0; j < 50; j ++)
		{
			if(tablaSegmentos[i].PAGINAS[j].MODIFICADO)
			{
				char* actualizacion = malloc(sizeof(int) + sizeof(int) + sizeof(int) + sizeof(tablaSegmentos[i].PAGINAS[j].KEY
						+ sizeof(tablaSegmentos[i].TABLA_ASOCIADA))
						+ strlen(tablaSegmentos[i].PAGINAS[j].VALUE));
				strcpy(actualizacion, (char*) 6);
				strcat(actualizacion, (char*) strlen(tablaSegmentos[i].TABLA_ASOCIADA));
				strcat(actualizacion, tablaSegmentos[i].TABLA_ASOCIADA);
				strcat(actualizacion, (char*) strlen( (char*)tablaSegmentos[i].PAGINAS[j].KEY));
				strcat(actualizacion, (char*) tablaSegmentos[i].PAGINAS[j].KEY);
				strcat(actualizacion, (char*) strlen(tablaSegmentos[i].PAGINAS[j].VALUE));
				strcat(actualizacion, tablaSegmentos[i].PAGINAS[j].VALUE);
				send(clienteFS, actualizacion, strlen(actualizacion), 0);
			}
		}
	}
	inicializarSegmentos();
	sem2 = true;
}

void realizarInsert(char* tabla, char* key, char* value)
{
	segmento segmentoAsociado = buscarSegmentoSegunTabla(tabla);
	if(segmentoAsociado.EN_USO != -1)
	{
		pagina paginaAsociada = buscarPagina(key, segmentoAsociado);
		if(paginaAsociada.EN_USO != -1)
		{
			paginaAsociada.MODIFICADO = 1;
			paginaAsociada.VALUE = value;
			paginaAsociada.TIMESTAMP = temporal_get_string_time();
		}
		else
		{
			insertarEnPaginaLibre(segmentoAsociado, key, value);
		}
	}
	else
	{
		segmentoAsociado = buscarSegmentoLibre();
		segmentoAsociado.PAGINAS[0].EN_USO = 1;
		segmentoAsociado.PAGINAS[0].KEY = key;
		segmentoAsociado.PAGINAS[0].MODIFICADO = 0;
		segmentoAsociado.PAGINAS[0].TIMESTAMP = temporal_get_string_time();
		segmentoAsociado.PAGINAS[0].VALUE = value;
	}
}

void realizarCreate(char* tabla, char* tipoConsistencia, char* numeroParticiones, char* tiempoCompactacion)
{
	char* mensaje = malloc(sizeof(int)+sizeof(int)+sizeof(int)+sizeof(int)+sizeof(int)+strlen(tabla)+strlen(tipoConsistencia)+strlen(numeroParticiones)+strlen(tiempoCompactacion));
	strcpy(mensaje, 3);
	strcat(mensaje, strlen(tabla));
	strcat(mensaje, tabla);
	strcat(mensaje, strlen(tipoConsistencia));
	strcat(mensaje, tipoConsistencia);
	strcat(mensaje, strlen(numeroParticiones));
	strcat(mensaje, numeroParticiones);
	strcat(mensaje, strlen(tiempoCompactacion));
	strcat(mensaje, tiempoCompactacion);
	send(clienteFS, mensaje, strlen(mensaje), 0);
}


