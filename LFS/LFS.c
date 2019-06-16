// FS
/*
 * HECHO: a verificar que ande:
 * - SELECT LEE TMPC
 * - SI NO ENCUENTRA LA KEY, ERROR
 *
 *
 * FALTANTES:
 * - ARREGLO DE RETORNO EN SELECT
 * - ARREGLO FUNCION APARTE MEMTABLE (rompe)
 * - LECTURA DE BLOQUES CON MAPEADO, POR SI NO ESTA TIME;KEY;VALUE COMPLETO
 *
 */
#include <stdio.h>
#include <string.h> //strlen
#include <stdlib.h>
#include <errno.h>
#include <unistd.h> //close
#include <arpa/inet.h> //close
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/time.h> //FD_SET, FD_ISSET, FD_ZERO macros
#include <ctype.h>
#include <sys/stat.h>
#include <dirent.h>
#include <time.h>
#include <pthread.h>
#include <commons/collections/dictionary.h>
#include <commons/config.h>
#include <commons/log.h>
#include <commons/string.h>
#include <commons/bitarray.h>
#include <sys/mman.h>
#include <fcntl.h>

#define TRUE 1
#define FALSE 0
//#define PORT 4444

typedef enum {
	SELECT, INSERT, CREATE, DESCRIBE, DROP, OPERACIONINVALIDA
} OPERACION;

typedef struct {
	int timestamp;
	int key;
	char* value;
} t_registro;

typedef struct {
	char *PUNTO_MONTAJE;
	int PUERTO_ESCUCHA;
	int RETARDO;
	int TAMANIO_VALUE;
	int TIEMPO_DUMP;
} configuracionLFS;

typedef struct {
	int PARTITIONS;
	char *CONSISTENCY;
	int COMPACTION_TIME;
} metadataTabla;

/*
 void iniciarConexion();
 void tomarPeticionSelect(int sd);
 void tomarPeticionCreate(int sd);
 void tomarPeticionInsert(int sd);
 */

t_dictionary * memtable; // creacion de memtable : diccionario que tiene las tablas como keys y su data es un array de p_registro 's.

metadataTabla describeUnaTabla(char *);
t_dictionary *describeTodasLasTablas();
void drop(char*);

void insert(char*, char*, char*, char*);
int existeUnaListaDeDatosADumpear();

char* realizarSelect(char*, char*);

void create(char*, char*, char*, char*);
void crearMetadata(char*, char*, char*, char*);
void crearBinarios(char*, int);
void asignarBloque(char*);

//Funciones Auxiliares
int existeCarpeta(char*);
int existeLaTabla(char*);
void tomarPeticion(char*);
void realizarPeticion(char**);
OPERACION tipoDePeticion(char*);
int cantidadValidaParametros(char**, int);
int parametrosValidos(int, char**, int (*criterioTiposCorrectos)(char**, int));
int esUnNumero(char* cadena);
int esUnTipoDeConsistenciaValida(char*);
int cantidadDeElementosDePunteroDePunterosDeChar(char** puntero);
//t_registro** obtenerDatosParaKeyDeseada(FILE *, int);
void obtenerDatosParaKeyDeseada(FILE *archivoBloque, int key,
		t_registro*vectorStructs[], int *cant);
void crearMetadataBloques();
int tamanioEnBytesDelBitarray();
//void actualizarBitArray();
void verBitArray();
void levantarFileSystem();
void levantarConfiguracionLFS();
void crearArchivoBitmap();
void iniciarMmap();
void crearArrayPorKeyMemtable(t_registro** arrayPorKeyDeseadaMemtable,
		t_registro **entradaTabla, int key, int *cant);
int estaEntreComillas(char*);

t_config* configLFS;
configuracionLFS structConfiguracionLFS;
t_bitarray* bitarrayBloques;
char *mmapDeBitmap;
t_dictionary* diccionarioDescribe;

int main(int argc, char *argv[]) {
	//pthread_t hiloLevantarConexion;
	levantarConfiguracionLFS();
	levantarFileSystem();
	iniciarMmap();
	bitarrayBloques = bitarray_create(mmapDeBitmap,
			tamanioEnBytesDelBitarray());
	//verBitArray();
	//pthread_create(&hiloLevantarConexion, NULL, iniciarConexion, NULL);

	memtable = malloc(4);
	memtable = dictionary_create();

	diccionarioDescribe = malloc(4000);
	diccionarioDescribe = dictionary_create();
	while (1) {
		printf("SELECT | INSERT | CREATE |\n");
		char* mensaje = malloc(1000);
		do {
			fgets(mensaje, 1000, stdin);
		} while (!strcmp(mensaje, "\n"));
		tomarPeticion(mensaje);
		free(mensaje);
		//verBitArray();
	}
	//Se queda esperando a que termine el hilo de escuchar peticiones
	//pthread_join(hiloLevantarConexion, NULL);

	//Aca se destruye el bitarray?
	//bitarray_destroy(bitarrayBloques);
	return 0;
}
void levantarFileSystem() {
	if (!existeCarpeta("Tables")) {
		mkdir(
				string_from_format("%sTables/",
						structConfiguracionLFS.PUNTO_MONTAJE), 0777);
	}
	if (!existeCarpeta("Bloques")) {
		mkdir(
				string_from_format("%sBloques/",
						structConfiguracionLFS.PUNTO_MONTAJE), 0777);
	}
	if (!existeCarpeta("Metadata")) {
		mkdir(
				string_from_format("%sMetadata/",
						structConfiguracionLFS.PUNTO_MONTAJE), 0777);
		//La metadata de bloques le defini algunos valores por defecto
		crearMetadataBloques();
		crearArchivoBitmap();
	}
}

void levantarConfiguracionLFS() {
	char *pathConfiguracion = string_new();
	//¿Cuando dice una ubicacion conocida se refiere a que esta hardcodeada asi?
	string_append(&pathConfiguracion, "configLFS.config");
	configLFS = config_create(pathConfiguracion);
	structConfiguracionLFS.PUERTO_ESCUCHA = config_get_int_value(configLFS,
			"PUERTO_ESCUCHA");
	//Lo que sigue abajo lo hago porque el punto de montaje ya tiene comillas, entonces se las tengo que sacar por
	//que sino queda ""home/carpeta""
	char *puntoMontaje = string_new();
	char *puntoMontajeSinComillas = string_new();
	string_append(&puntoMontaje,
			config_get_string_value(configLFS, "PUNTO_MONTAJE"));
	//saco la doble comilla del principio y la del final
	string_append(&puntoMontajeSinComillas,
			string_substring(puntoMontaje, 1, strlen(puntoMontaje) - 2));

	structConfiguracionLFS.PUNTO_MONTAJE = puntoMontajeSinComillas;
	structConfiguracionLFS.TAMANIO_VALUE = config_get_int_value(configLFS,
			"TAMANIO_VALUE");
	//Los 2 valores que siguen tienen que poder modificarse en tiempo de ejecucion
	//asi que tendria que volver a tomar su valor cuando los vaya a usar
	structConfiguracionLFS.RETARDO = config_get_int_value(configLFS, "RETARDO");
	structConfiguracionLFS.TIEMPO_DUMP = config_get_int_value(configLFS,
			"TIEMPO_DUMP");
	config_destroy(configLFS);
}

void tomarPeticion(char* mensaje) {
	//Fijarse despues cual seria la cantidad correcta de malloc
	char** mensajeSeparado = malloc(strlen(mensaje) + 1);
	mensajeSeparado = string_split(mensaje, " \n");
	realizarPeticion(mensajeSeparado);
	free(mensajeSeparado);
}

//Mejor forma de hacer esto?
int cantidadDeElementosDePunteroDePunterosDeChar(char** puntero) {
	int i = 0;
	while (puntero[i] != NULL) {
		i++;
	}
	//Uno mas porque tambien se incluye el NULL en el vector
	return ++i;
}

void realizarPeticion(char** parametros) {
	char *peticion = parametros[0];
	OPERACION instruccion = tipoDePeticion(peticion);
	switch (instruccion) {
	case SELECT:
		printf("Seleccionaste Select\n");
		//Defino de que manera van a ser validos los parametros del select y luego paso el puntero de dicha funcion.
		//Los parametros son validos si el segundo (la key) es un numero, y la cantidadDeParametrosUsados solo se pasa para hacer
		//polimorfica la funcion criterioTiposCorrectos.
		int criterioSelect(char** parametros, int cantidadDeParametrosUsados) {
			char* key = parametros[2];
			if (!esUnNumero(key)) {
				printf("La key debe ser un numero.\n");
			}
			return esUnNumero(key);
		}

		if (parametrosValidos(2, parametros, (void*) criterioSelect)) {
			char* tabla = parametros[1];
			char* key = parametros[2];
			realizarSelect(tabla, key);
		}

		break;

	case INSERT:
		printf("Seleccionaste Insert\n");
		int criterioInsert(char** parametros, int cantidadDeParametrosUsados) {
			char* key = parametros[2];
			char* value = parametros[3];
			if (!esUnNumero(key)) {
				printf("La key debe ser un numero.\n");
			}
			if (!estaEntreComillas(value)) {
				printf("El valor a insertar debe estar entre comillas.\n");
			}

			if (cantidadDeParametrosUsados == 4) {
				char* timestamp = parametros[4];
				if (!esUnNumero(timestamp)) {
					printf("El timestamp debe ser un numero.\n");
				}
				return esUnNumero(key) && esUnNumero(timestamp)
						&& estaEntreComillas(value);
			}
			return esUnNumero(key);
		}
		//puede o no estar el timestamp
		if (parametrosValidos(4, parametros, (void *) criterioInsert)) {
			char *tabla = parametros[1];
			char *key = parametros[2];
			//le saco las comillas al valor
			char *valor = string_substring(parametros[3], 1,
					string_length(parametros[3]) - 2);
			char *timestamp = parametros[4];
			//printf("%s\n", valor);
			insert(tabla, key, valor, timestamp);

		} else if (parametrosValidos(3, parametros, (void *) criterioInsert)) {
			char *tabla = parametros[1];
			char *key = parametros[2];
			//le saco las comillas al valor
			char *valor = string_substring(parametros[3], 1,
					string_length(parametros[3]) - 2);
			//¿El timestamp nesecita conversion? esto esta en segundos y no hay tipo de dato que banque los milisegundos por el tamanio
			long int timestampActual = (long int) time(NULL);
			char* timestamp = string_itoa(timestampActual);
			insert(tabla, key, valor, timestamp);
		}
		break;
	case CREATE:
		printf("Seleccionaste Create\n");
		int criterioCreate(char** parametros, int cantidadDeParametrosUsados) {
			char* tiempoCompactacion = parametros[4];
			char* cantidadParticiones = parametros[3];
			char* consistencia = parametros[2];
			if (!esUnNumero(cantidadParticiones)) {
				printf("La cantidad de particiones debe ser un numero.\n");
			}
			if (!esUnNumero(tiempoCompactacion)) {
				printf("El tiempo de compactacion debe ser un numero.\n");
			}
			return esUnNumero(cantidadParticiones)
					&& esUnNumero(tiempoCompactacion)
					&& esUnTipoDeConsistenciaValida(consistencia);
		}
		if (parametrosValidos(4, parametros, (void *) criterioCreate)) {
			char* tabla = parametros[1];
			char* tiempoCompactacion = parametros[4];
			char* cantidadParticiones = parametros[3];
			char* consistencia = parametros[2];
			create(tabla, consistencia, cantidadParticiones,
					tiempoCompactacion);
		}
		break;
	case DROP:
		printf("Seleccionaste Drop\n");
		int criterioDrop(char** parametros, int cantidadDeParametrosUsados) {
			char* tabla = parametros[1];
			string_to_upper(tabla);
			if (!existeLaTabla(tabla)) {
				printf("La tabla a borrar no existe\n");
			}
			return existeLaTabla(tabla);
		}
		if (parametrosValidos(1, parametros, (void *) criterioDrop)) {
			char* tabla = parametros[1];
			string_to_upper(tabla);
			drop(tabla);
		}
		break;
	case DESCRIBE:
		printf("Seleccionaste Describe\n");
		int criterioDescribeTodasLasTablas(char** parametros,
				int cantidadDeParametrosUsados) {
			char* tabla = parametros[1];
			return (tabla == NULL);
		}
		int criterioDescribeUnaTabla(char** parametros,
				int cantidadDeParametrosUsados) {
			char* tabla = parametros[1];
			if (tabla == NULL) {
				return 0;
			}
			string_to_upper(tabla);
			if(!existeLaTabla(tabla)){
				printf("La tabla no existe\n");
			}
			return existeLaTabla(tabla);
		}
		if (parametrosValidos(0, parametros,
				(void *) criterioDescribeTodasLasTablas)) {
			describeTodasLasTablas();
		}
		if (parametrosValidos(1, parametros,
				(void *) criterioDescribeUnaTabla)) {
			char* tabla = parametros[1];
			string_to_upper(tabla);
			describeUnaTabla(tabla);
		}
		break;
	default:
		printf("Error operacion invalida\n");
	}
}

int parametrosValidos(int cantidadDeParametrosNecesarios, char** parametros,
		int (*criterioTiposCorrectos)(char**, int)) {
	return cantidadValidaParametros(parametros, cantidadDeParametrosNecesarios)
			&& criterioTiposCorrectos(parametros,
					cantidadDeParametrosNecesarios);;
}

int cantidadValidaParametros(char** parametros,
		int cantidadDeParametrosNecesarios) {
	//Saco de la cuenta la peticion y el NULL
	int cantidadDeParametrosQueTengo =
			cantidadDeElementosDePunteroDePunterosDeChar(parametros) - 2;
	if (cantidadDeParametrosQueTengo != cantidadDeParametrosNecesarios) {
		//hay que arreglar esto para que en el caso de insert solo lo muestre si no se cumple con 4 ni con 3
		//printf("La cantidad de parametros no es valida\n");
		return 0;
	}
	return 1;
}

OPERACION tipoDePeticion(char* peticion) {
	string_to_upper(peticion);
	if (!strcmp(peticion, "SELECT")) {
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
				if (!strcmp(peticion, "DROP")) {
					free(peticion);
					return DROP;
				} else {
					if (!strcmp(peticion, "DESCRIBE")) {
						free(peticion);
						return DESCRIBE;
					} else {
						free(peticion);
						return OPERACIONINVALIDA;
					}
				}
			}
		}
	}
}

int esUnNumero(char* cadena) {
	for (int i = 0; i < strlen(cadena); i++) {
		if (!isdigit(cadena[i])) {
			return 0;
		}
	}
	return 1;
}

int esUnTipoDeConsistenciaValida(char* cadena) {
	int consistenciaValida = !strcmp(cadena, "SC") || !strcmp(cadena, "SHC")
			|| !strcmp(cadena, "EC");
	if (!consistenciaValida) {
		printf(
				"El tipo de consistencia no es valida. Asegurese de que este en mayusculas\n");
	}
	return consistenciaValida;
}

void insert(char* tabla, char* key, char* valor, char* timestamp) {
	string_to_upper(tabla);
	if (!existeLaTabla(tabla)) {
		char* mensajeALogear = malloc(
				strlen("Error: no existe una tabla con el nombre ")
						+ strlen(tabla) + 1);
		strcpy(mensajeALogear, "Error: no existe una tabla con el nombre ");
		strcat(mensajeALogear, tabla);
		t_log* g_logger;
		//Si uso LOG_LEVEL_ERROR no lo imprime ni lo escribe. ¿Esto deberia guardarlo en un .log?
		g_logger = log_create(
				string_from_format("%s/erroresInsert.log",
						structConfiguracionLFS.PUNTO_MONTAJE), "LFS", 1,
				LOG_LEVEL_INFO);
		log_error(g_logger, mensajeALogear);
		log_destroy(g_logger);
		free(mensajeALogear);
	} else {
		t_registro* p_registro = malloc(12); // 2 int = 2* 4        +       un puntero a char = 4
		p_registro->timestamp = atoi(timestamp);
		p_registro->key = atoi(key);
		p_registro->value = malloc(strlen(valor));
		strcpy(p_registro->value, valor);
		if (!existeUnaListaDeDatosADumpear(tabla)) {
			t_registro* p_registro = malloc(12); // 2 int = 2* 4        +       un puntero a char = 4
			p_registro->timestamp = atoi(timestamp);
			p_registro->key = atoi(key);
			p_registro->value = malloc(strlen(valor));
			strcpy(p_registro->value, valor);
			t_registro* vectorStructs[100];
			vectorStructs[0] = malloc(12);
			memcpy(&vectorStructs[0]->key, &p_registro->key,
					sizeof(p_registro->key));
			memcpy(&vectorStructs[0]->timestamp, &p_registro->timestamp,
					sizeof(p_registro->timestamp));
			vectorStructs[0]->value = malloc(strlen(p_registro->value));
			memcpy(vectorStructs[0]->value, p_registro->value,
					strlen(p_registro->value));

			dictionary_put(memtable, tabla, &vectorStructs);
			// t_registro **existe = dictionary_get(memtable, "TABLA1");
			// printf("%s\n",existe[0]->value);

		} else {
			t_registro **vectorStructs = dictionary_get(memtable, tabla);
			int i;
			for (i = 0; i < 100; i++) {
				if (vectorStructs[i] == NULL) {
					break;
				}
			}
			vectorStructs[i] = malloc(12);
			memcpy(&vectorStructs[i]->key, &p_registro->key,
					sizeof(p_registro->key));
			memcpy(&vectorStructs[i]->timestamp, &p_registro->timestamp,
					sizeof(p_registro->timestamp));
			vectorStructs[i]->value = malloc(strlen(p_registro->value));
			memcpy(vectorStructs[i]->value, p_registro->value,
					strlen(p_registro->value));

			dictionary_put(memtable, tabla, vectorStructs);
		}
	}
}

int existeUnaListaDeDatosADumpear(char* tabla) {
	return dictionary_has_key(memtable, tabla);
}

void create(char* tabla, char* consistencia, char* cantidadDeParticiones,
		char* tiempoDeCompactacion) {
	string_to_upper(tabla);
	if (existeLaTabla(tabla)) {
		char* mensajeALogear = malloc(
				strlen("Error: ya existe una tabla con el nombre ")
						+ strlen(tabla) + 1);
		strcpy(mensajeALogear, "Error: ya existe una tabla con el nombre ");
		strcat(mensajeALogear, tabla);
		t_log* g_logger;
		//Si uso LOG_LEVEL_ERROR no lo imprime ni lo escribe
		g_logger = log_create(
				string_from_format("%serroresCreate.log",
						structConfiguracionLFS.PUNTO_MONTAJE), "LFS", 1,
				LOG_LEVEL_INFO);
		log_error(g_logger, mensajeALogear);
		log_destroy(g_logger);
		free(mensajeALogear);
	} else {
		char* path = malloc(
				strlen(
						string_from_format("%sTables/",
								structConfiguracionLFS.PUNTO_MONTAJE))
						+ strlen(tabla) + 1);
		char* metadataPath = malloc(
				strlen(
						string_from_format("%sTables/",
								structConfiguracionLFS.PUNTO_MONTAJE))
						+ strlen(tabla) + strlen("/metadata") + 1);
		strcpy(path,
				string_from_format("%sTables/",
						structConfiguracionLFS.PUNTO_MONTAJE));
		strcat(path, tabla);
		//El segundo parametro es una mascara que define permisos
		mkdir(path, 0777);

		strcpy(metadataPath, path);
		strcat(metadataPath, "/metadata");
		//Si en algun momento quiero convertir string a int existe la funcion atoi
		crearMetadata(metadataPath, consistencia, cantidadDeParticiones,
				tiempoDeCompactacion);
		crearBinarios(path, atoi(cantidadDeParticiones));
		free(metadataPath);
		free(path);
	}
}

void crearBinarios(char* path, int cantidadDeParticiones) {
	for (int i = 0; i < cantidadDeParticiones; i++) {
		//10 para dejar cierto margen a la cantidad de digitos de las particiones
		char* directorioBinario = malloc(strlen(path) + 10);
		char* numeroDeParticion = string_itoa(i);
		strcpy(directorioBinario, path);
		strcat(directorioBinario, "/");
		strcat(directorioBinario, numeroDeParticion);
		strcat(directorioBinario, ".bin");
		asignarBloque(directorioBinario);
		free(directorioBinario);
		free(numeroDeParticion);
	}
}

//No se como funciona esta parte
void asignarBloque(char* directorioBinario) {
	//resolver lo mismo del save y el create que paso con el crearMetadata
	FILE *archivoBinario = fopen(directorioBinario, "w");
	t_config *binario = config_create(directorioBinario);
	int encontroUnBloque = 0;
	int bloqueEncontrado = 0;
	//Reemplazar para que vaya hasta la cantidad de bloques del archivo de config
	for (int i = 0; i < bitarray_get_max_bit(bitarrayBloques); i++) {
		if (bitarray_test_bit(bitarrayBloques, i) == 0) {
			bitarray_set_bit(bitarrayBloques, i);
			encontroUnBloque = 1;
			bloqueEncontrado = i;
			break;
		}
	}

	if (encontroUnBloque) {
		char *stringdelArrayDeBloques = string_new();
		string_append(&stringdelArrayDeBloques, "[");
		string_append(&stringdelArrayDeBloques, string_itoa(bloqueEncontrado));
		string_append(&stringdelArrayDeBloques, "]");
		//64 lo tengo que reemplazar por el tamanio de un bloque supongo
		config_set_value(binario, "SIZE", "64");
		//los bloques despues se levantan con config_get_array_value
		config_set_value(binario, "BLOCKS", stringdelArrayDeBloques);
		config_save_in_file(binario, directorioBinario);
		//actualizarBitArray();
		//creo el archivo .bin del bloque
		char* pathBloque = string_new();
		string_append(&pathBloque,
				string_from_format("%sBloques/",
						structConfiguracionLFS.PUNTO_MONTAJE));
		string_append(&pathBloque, string_itoa(bloqueEncontrado));
		string_append(&pathBloque, ".bin");
		FILE *bloqueCreado = fopen(pathBloque, "w");
		fclose(bloqueCreado);
	}

	else {
		//¿Que deberia hacer en este caso?
		printf("No se encontro bloque disponible\n");
	}

	fclose(archivoBinario);
	config_destroy(binario);
}

void verBitArray() {
	printf("%i -- ", bitarray_get_max_bit(bitarrayBloques));
	for (int j = 0; j < bitarray_get_max_bit(bitarrayBloques); j++) {

		bool bit = bitarray_test_bit(bitarrayBloques, j);
		printf("%i", bit);
	}
	printf("\n");
}

//la cantidad de bloques dividido por 8 bits = cantidad de bytes necesarios
//¿Cuantos bloques genero con 649 bytes? como cada bloque es un bit, 649 bytes * 8 bits = 5192 bloques
int tamanioEnBytesDelBitarray() {
	char *metadataPath = string_from_format("%sMetadata/metadata.bin",
			structConfiguracionLFS.PUNTO_MONTAJE);
	t_config *metadata = config_create(metadataPath);
	//int tamanioPorBloque = config_get_int_value(metadata, "BLOCK_SIZE");
	int cantidadDeBloques = config_get_int_value(metadata, "BLOCKS");
	config_destroy(metadata);
	//¿Que pasa si la cantidad de bloques no es divisible por 8?
	return cantidadDeBloques / 8;
}

void crearMetadataBloques() {
	char *metadataPath = string_new();
	string_append(&metadataPath,
			string_from_format("%sMetadata/metadata.bin",
					structConfiguracionLFS.PUNTO_MONTAJE));
	FILE *archivoMetadata = fopen(metadataPath, "w");
	t_config *metadata = config_create(metadataPath);
	//Estos datos harcodeados despues tienen que modificarse. ¿Tienen que tener algun valor especial por defecto?
	config_set_value(metadata, "BLOCK_SIZE", "128");
	//4096
	config_set_value(metadata, "BLOCKS", "512");
	config_set_value(metadata, "MAGIC_NUMBER", "LISSANDRA");
	config_save_in_file(metadata, metadataPath);
	fclose(archivoMetadata);
	config_destroy(metadata);
}

void crearArchivoBitmap() {
	char *pathBitmap = string_new();
	string_append(&pathBitmap, structConfiguracionLFS.PUNTO_MONTAJE);
	string_append(&pathBitmap, "Metadata/bitmap.bin");
	FILE *f = fopen(pathBitmap, "w");

	//5192 sale de mystat.st_size (tamanio del archivo de bitmap). tengo que escribir en el archivo para mapear la memoria.
	for (int i = 0; i < 5192; i++) {
		fputc(0, f);
	}

	fclose(f);
}

//Preguntar sobre esto
void iniciarMmap() {
	char *pathBitmap = string_new();
	string_append(&pathBitmap, structConfiguracionLFS.PUNTO_MONTAJE);
	string_append(&pathBitmap, "Metadata/bitmap.bin");
	//Open es igual a fopen pero el FD me lo devuelve como un int (que es lo que necesito para fstat)
	int bitmap = open(pathBitmap, O_RDWR);
	struct stat mystat;

	//fstat rellena un struct con informacion del FD dado
	if (fstat(bitmap, &mystat) < 0) {
		close(bitmap);
	}

	/*	mmap mapea un archivo en memoria y devuelve la direccion de memoria donde esta ese mapeo
	 *  MAP_SHARED Comparte este área con todos los otros  objetos  que  señalan  a  este  objeto.
	 Almacenar  en  la  región  es equivalente a escribir en el fichero.
	 */
	mmapDeBitmap = mmap(NULL, mystat.st_size, PROT_WRITE | PROT_READ,
	MAP_SHARED, bitmap, 0);
	close(bitmap);

}

void crearMetadata(char* metadataPath, char* consistencia,
		char* cantidadDeParticiones, char* tiempoDeCompactacion) {
	FILE *archivoMetadata = fopen(metadataPath, "w");
	t_config *metadata = config_create(metadataPath);
	config_set_value(metadata, "CONSISTENCY", consistencia);
	config_set_value(metadata, "PARTITIONS", cantidadDeParticiones);
	config_set_value(metadata, "COMPACTION_TIME", tiempoDeCompactacion);
	//config_save_in_file necesita el t_config creado que a su vez el config_create
	//necesita el archivo que se crea en el save por eso lo creo yo no se si esta bien
	config_save_in_file(metadata, metadataPath);
	fclose(archivoMetadata);
	config_destroy(metadata);
}

//Las funciones de abajo repiten logica, si hay tiempo hacer una funcion sola
int existeLaTabla(char* nombreDeTabla) {
	DIR *directorio = opendir(
			string_from_format("%sTables",
					structConfiguracionLFS.PUNTO_MONTAJE));
	struct dirent *directorioALeer;
	while ((directorioALeer = readdir(directorio)) != NULL) {
		//Evaluo si de todas las carpetas dentro de TABLAS existe alguna que tenga el mismo nombre
		if ((directorioALeer->d_type) == DT_DIR
				&& !strcmp((directorioALeer->d_name), nombreDeTabla)) {
			closedir(directorio);
			return 1;
		}
	}
	closedir(directorio);
	return 0;
}

void drop(char* tabla) {
	char *path = string_new();
	string_append(&path,
			string_from_format("%sTables/",
					structConfiguracionLFS.PUNTO_MONTAJE));
	string_append(&path, tabla);
	DIR *directorio = opendir(path);
	printf("%s\n\n", path);
	struct dirent *directorioALeer;
	while ((directorioALeer = readdir(directorio)) != NULL) {
		//printf("- %s\n", directorioALeer->d_name);
		//Elimino todo lo que tiene adentro la tabla
		if (!((directorioALeer->d_type) == DT_DIR)) {
			char *archivoABorrar = string_new();
			string_append(&archivoABorrar, path);
			string_append(&archivoABorrar, "/");
			string_append(&archivoABorrar, directorioALeer->d_name);
			printf("!!%s\n", archivoABorrar);
			remove(archivoABorrar);
		}
	}
	rmdir(path);
	closedir(directorio);
}

int existeCarpeta(char *nombreCarpeta) {
	DIR *directorio = opendir(structConfiguracionLFS.PUNTO_MONTAJE);
	if (directorio == NULL) {
		mkdir(structConfiguracionLFS.PUNTO_MONTAJE, 0777);
		return 0;
	}
	struct dirent *directorioALeer;
	while ((directorioALeer = readdir(directorio)) != NULL) {
		//Evaluo si de todas las carpetas dentro de TABAS existe alguna que tenga el mismo nombre
		if ((directorioALeer->d_type) == DT_DIR
				&& !strcmp((directorioALeer->d_name), nombreCarpeta)) {
			closedir(directorio);
			return 1;
		}
	}
	closedir(directorio);
	return 0;
}

//No le pongo "select" porque ya esta la funcion de socket y rompe
char* realizarSelect(char* tabla, char* key) {
	string_to_upper(tabla);
	if (existeLaTabla(tabla)) {
		char* pathMetadata = malloc(
				strlen(
						string_from_format("%sTables/",
								structConfiguracionLFS.PUNTO_MONTAJE))
						+ strlen(tabla) + strlen("/metadata") + 1);
		strcpy(pathMetadata,
				string_from_format("%sTables/",
						structConfiguracionLFS.PUNTO_MONTAJE));
		strcat(pathMetadata, tabla);
		strcat(pathMetadata, "/metadata");
		t_config *metadata = config_create(pathMetadata);
		int cantidadDeParticiones = config_get_int_value(metadata,
				"PARTITIONS");

		int particionQueContieneLaKey = (atoi(key)) % cantidadDeParticiones;
		printf("La key esta en la particion %i\n", particionQueContieneLaKey);
		char* stringParticion = malloc(4);
		stringParticion = string_itoa(particionQueContieneLaKey);

		char* pathParticionQueContieneKey = malloc(
				strlen(
						string_from_format("%sTables/",
								structConfiguracionLFS.PUNTO_MONTAJE))
						+ strlen(tabla) + strlen("/") + strlen(stringParticion)
						+ strlen(".bin") + 1);
		strcpy(pathParticionQueContieneKey,
				string_from_format("%sTables/",
						structConfiguracionLFS.PUNTO_MONTAJE));
		strcat(pathParticionQueContieneKey, tabla);
		strcat(pathParticionQueContieneKey, "/");
		strcat(pathParticionQueContieneKey, stringParticion);
		strcat(pathParticionQueContieneKey, ".bin");
		t_config *tamanioYBloques = config_create(pathParticionQueContieneKey);
		char** vectorBloques = config_get_array_value(tamanioYBloques, "BLOCK"); //devuelve vector de STRINGS

		int m = 0;
		while (vectorBloques[m] != NULL) {
			m++;
		}

		int timestampActualMayorBloques = -1;
		char* valueDeTimestampActualMayorBloques = string_new();

		// POR CADA BLOQUE, TENGO QUE ENTRAR A ESTE BLOQUE
		for (int i = 0; i < m; i++) {
			char* pathBloque = malloc(
					strlen(
							string_from_format("%sBloques/",
									structConfiguracionLFS.PUNTO_MONTAJE))
							+ strlen((vectorBloques[i])) + strlen(".bin") + 1);
			strcpy(pathBloque, "./Bloques/");
			strcat(pathBloque, vectorBloques[i]);
			strcat(pathBloque, ".bin");
			FILE *archivoBloque = fopen(pathBloque, "r");
			if (archivoBloque == NULL) {
				printf("no se pudo abrir archivo de bloques");
				exit(1);
			}

			int cantidadIgualDeKeysEnBloque = 0;
			t_registro* vectorStructs[100];
			obtenerDatosParaKeyDeseada(archivoBloque, (atoi(key)),
					vectorStructs, &cantidadIgualDeKeysEnBloque);

			//printf("%i", vectorStructs[0]->timestamp);
			//printf("%i", vectorStructs[1]->timestamp);

			//cual de estos tiene el timestamp mas grande? guardar timestamp y value
			int temp = 0;
			char* valor;
			for (int k = 1; k < cantidadIgualDeKeysEnBloque; k++) {
				for (int j = 0; j < (cantidadIgualDeKeysEnBloque - k); j++) {
					if (vectorStructs[j]->timestamp
							< vectorStructs[j + 1]->timestamp) {
						temp = vectorStructs[j + 1]->timestamp;
						valor = malloc(strlen(vectorStructs[j + 1]->value));
						strcpy(valor, vectorStructs[j + 1]->value);

						vectorStructs[j + 1]->timestamp =
								vectorStructs[j]->timestamp;
						vectorStructs[j + 1]->value = vectorStructs[j]->value;

						vectorStructs[j]->timestamp = temp;
						vectorStructs[j]->value = valor;
					}
				}
			} // aca quedaria el vector ordenado por timestamp mayor

			if (vectorStructs[0]->timestamp > timestampActualMayorBloques) {
				timestampActualMayorBloques = vectorStructs[0]->timestamp;
				strcpy(valueDeTimestampActualMayorBloques, "");
				string_append(&valueDeTimestampActualMayorBloques,
						vectorStructs[0]->value);
			}
			fclose(archivoBloque);
			free(pathBloque);
			//free(vectorStructs);
		} 	//cierra el for

		// si encontro alguno, me guarda el timestamp mayor en timestampActualMayorBloques
		// y guarda el valor en valueDeTimestampActualMayorBloques
		// si no hay ninguno en vectorBloques (porque por ej, esta en los temporales)
		// entonces timestampActualMayorBloques = -1 y
		// valueDeTimestampActualMayorBloques = NULL

		//-------------------------------------------------

		// AHORA ABRO ARCHIVOS TEMPORALES. EL PROCEDIMIENTO ES MUY PARECIDO AL ANTERIOR
		char* pathTemporales = malloc(
				strlen(
						string_from_format("%sTables/",
								structConfiguracionLFS.PUNTO_MONTAJE))
						+ strlen(tabla) + 1);
		strcpy(pathTemporales,
				string_from_format("%sTables/",
						structConfiguracionLFS.PUNTO_MONTAJE));
		strcat(pathTemporales, tabla);

		DIR *directorioTemporal = opendir(pathTemporales);
		struct dirent *archivoALeer;

		int timestampActualMayorTemporales = -1;
		char* valueDeTimestampActualMayorTemporales = string_new();

		while ((archivoALeer = readdir(directorioTemporal)) != NULL) { //PARA CADA ARCHIVO DE LA TABLA ESPECIFICA
			if (string_ends_with(archivoALeer->d_name, ".tmp")) {

				//obtengo el nombre de ese archivo .tmp . Ejemplo obtengo A1.tmp siendo A1 el nombre (tipo char*)
				char* nombreArchivoTemporal = string_split(archivoALeer->d_name,
						".")[0];
				// ahora ya tengo el nombre del archivo .tmp

				char* pathTemporal = malloc(
						strlen(
								string_from_format("%sTables/",
										structConfiguracionLFS.PUNTO_MONTAJE))
								+ strlen(tabla) + strlen("/")
								+ strlen(nombreArchivoTemporal) + strlen(".tmp")
								+ 1);
				strcpy(pathTemporal,
						string_from_format("%sTables/",
								structConfiguracionLFS.PUNTO_MONTAJE));
				strcat(pathTemporal, tabla);
				strcat(pathTemporal, "/");
				strcat(pathTemporal, nombreArchivoTemporal);
				strcat(pathTemporal, ".tmp");
				FILE *fileTemporal = fopen(pathTemporal, "r");
				if (fileTemporal == NULL) {
					printf("no se pudo abrir archivo de temporales");
					exit(1);
				}

				t_config *tamanioYBloquesTmp = config_create(pathTemporal);
				char** vectorBloquesTmp = config_get_array_value(
						tamanioYBloquesTmp, "BLOCK"); //devuelve vector de STRINGS

				int n = 0;
				while (vectorBloquesTmp[n] != NULL) {
					n++;
				}

				//POR CADA BLOQUE, TENGO QUE ENTRAR A ESE BLOQUE
				for (int q = 0; q < n; q++) {
					char* pathBloqueTmp =
							malloc(
									strlen(
											string_from_format("%sBloques/",
													structConfiguracionLFS.PUNTO_MONTAJE))
											+ strlen((vectorBloques[q]))
											+ strlen(".bin") + 1);
					strcpy(pathBloqueTmp,
							string_from_format("%sBloques/",
									structConfiguracionLFS.PUNTO_MONTAJE));
					strcat(pathBloqueTmp, vectorBloquesTmp[q]);
					strcat(pathBloqueTmp, ".bin");
					FILE *archivoBloqueTmp = fopen(pathBloqueTmp, "r");
					if (archivoBloqueTmp == NULL) {
						printf("no se pudo abrir archivo de bloques");
						exit(1);
					}

					int cantidadIgualDeKeysEnTemporal = 0;
					t_registro* vectorStructsTemporal[100];
					obtenerDatosParaKeyDeseada(archivoBloqueTmp, (atoi(key)),
							vectorStructsTemporal,
							&cantidadIgualDeKeysEnTemporal);

					//cual de estos tiene el timestamp mas grande? guardar timestamp y value
					int tempo = 0;
					char* valorTemp;
					for (int k = 1; k < cantidadIgualDeKeysEnTemporal; k++) {
						for (int j = 0; j < (cantidadIgualDeKeysEnTemporal - k);
								j++) {
							if (vectorStructsTemporal[j]->timestamp
									< vectorStructsTemporal[j + 1]->timestamp) {
								tempo = vectorStructsTemporal[j + 1]->timestamp;
								valorTemp =
										malloc(
												strlen(
														vectorStructsTemporal[j
																+ 1]->value));
								strcpy(valorTemp,
										vectorStructsTemporal[j + 1]->value);

								vectorStructsTemporal[j + 1]->timestamp =
										vectorStructsTemporal[j]->timestamp;
								vectorStructsTemporal[j + 1]->value =
										vectorStructsTemporal[j]->value;

								vectorStructsTemporal[j]->timestamp = tempo;
								vectorStructsTemporal[j]->value = valorTemp;
							}
						}
					}

					if (vectorStructsTemporal[0]->timestamp
							> timestampActualMayorTemporales) {
						timestampActualMayorTemporales =
								vectorStructsTemporal[0]->timestamp;
						strcpy(valueDeTimestampActualMayorTemporales, "");
						string_append(&valueDeTimestampActualMayorTemporales,
								vectorStructsTemporal[0]->value);
					}
					fclose(archivoBloqueTmp);
					free(pathBloqueTmp);
					//free(vectorStructsTemporal);
				} // cierra el for
			} // cierra el if
		} //cierra el while

		// si encontro alguno, me guarda el timestamp mayor en timestampActualMayorTemporales
		// y guarda el valor en valueDeTimestampActualMayorTemporales
		// si no hay ninguno en vectorStructsTemporal
		// entonces timestampActualMayorTemporales = -1 y
		// valueDeTimestampActualMayorTemporales = NULL

		closedir(directorioTemporal);

		//-------------------------------------------------

		// AHORA ABRO ARCHIVOS TEMPC DE COMPACTACION. EL PROCEDIMIENTO ES MUY PARECIDO AL ANTERIOR
		char* pathTemporalesC = malloc(
				strlen(
						string_from_format("%sTables/",
								structConfiguracionLFS.PUNTO_MONTAJE))
						+ strlen(tabla) + 1);
		strcpy(pathTemporalesC,
				string_from_format("%sTables/",
						structConfiguracionLFS.PUNTO_MONTAJE));
		strcat(pathTemporalesC, tabla);

		DIR *directorioTemporalC = opendir(pathTemporalesC);
		struct dirent *archivoCALeer;

		int timestampActualMayorTemporalesC = -1;
		char* valueDeTimestampActualMayorTemporalesC = string_new();

		while ((archivoCALeer = readdir(directorioTemporalC)) != NULL) { //PARA CADA ARCHIVO DE LA TABLA ESPECIFICA
			if (string_ends_with(archivoCALeer->d_name, ".tmpc")) {

				//obtengo el nombre de ese archivo .tmpc . Ejemplo obtengo A1.tmp siendo A1 el nombre (tipo char*)
				char* nombreArchivoTemporalC = string_split(
						archivoCALeer->d_name, ".")[0];
				// ahora ya tengo el nombre del archivo .tmpc

				char* pathTemporalC = malloc(
						strlen(
								string_from_format("%sTables/",
										structConfiguracionLFS.PUNTO_MONTAJE))
								+ strlen(tabla) + strlen("/")
								+ strlen(nombreArchivoTemporalC)
								+ strlen(".tmpc") + 1);
				strcpy(pathTemporalC,
						string_from_format("%sTables/",
								structConfiguracionLFS.PUNTO_MONTAJE));
				strcat(pathTemporalC, tabla);
				strcat(pathTemporalC, "/");
				strcat(pathTemporalC, nombreArchivoTemporalC);
				strcat(pathTemporalC, ".tmpc");
				FILE *fileTemporalC = fopen(pathTemporalC, "r");
				if (fileTemporalC == NULL) {
					printf("no se pudo abrir archivo de temporales");
					exit(1);
				}

				t_config *tamanioYBloquesTmpC = config_create(pathTemporalC);
				char** vectorBloquesTmpC = config_get_array_value(
						tamanioYBloquesTmpC, "BLOCK"); //devuelve vector de STRINGS

				int n = 0;
				while (vectorBloquesTmpC[n] != NULL) {
					n++;
				}

				//POR CADA BLOQUE, TENGO QUE ENTRAR A ESE BLOQUE
				for (int q = 0; q < n; q++) {
					char* pathBloqueTmpC =
							malloc(
									strlen(
											string_from_format("%sBloques/",
													structConfiguracionLFS.PUNTO_MONTAJE))
											+ strlen((vectorBloques[q]))
											+ strlen(".bin") + 1);
					strcpy(pathBloqueTmpC,
							string_from_format("%sBloques/",
									structConfiguracionLFS.PUNTO_MONTAJE));
					strcat(pathBloqueTmpC, vectorBloquesTmpC[q]);
					strcat(pathBloqueTmpC, ".bin");
					FILE *archivoBloqueTmpC = fopen(pathBloqueTmpC, "r");
					if (archivoBloqueTmpC == NULL) {
						printf("no se pudo abrir archivo de bloques");
						exit(1);
					}

					int cantidadIgualDeKeysEnTemporal = 0;
					t_registro* vectorStructsTemporalC[100];
					obtenerDatosParaKeyDeseada(archivoBloqueTmpC, (atoi(key)),
							vectorStructsTemporalC,
							&cantidadIgualDeKeysEnTemporal);

					//cual de estos tiene el timestamp mas grande? guardar timestamp y value
					int tempo = 0;
					char* valorTempC;
					for (int k = 1; k < cantidadIgualDeKeysEnTemporal; k++) {
						for (int j = 0; j < (cantidadIgualDeKeysEnTemporal - k);
								j++) {
							if (vectorStructsTemporalC[j]->timestamp
									< vectorStructsTemporalC[j + 1]->timestamp) {
								tempo =
										vectorStructsTemporalC[j + 1]->timestamp;
								valorTempC =
										malloc(
												strlen(
														vectorStructsTemporalC[j
																+ 1]->value));
								strcpy(valorTempC,
										vectorStructsTemporalC[j + 1]->value);

								vectorStructsTemporalC[j + 1]->timestamp =
										vectorStructsTemporalC[j]->timestamp;
								vectorStructsTemporalC[j + 1]->value =
										vectorStructsTemporalC[j]->value;

								vectorStructsTemporalC[j]->timestamp = tempo;
								vectorStructsTemporalC[j]->value = valorTempC;
							}
						}
					}

					if (vectorStructsTemporalC[0]->timestamp
							> timestampActualMayorTemporalesC) {
						timestampActualMayorTemporalesC =
								vectorStructsTemporalC[0]->timestamp;
						strcpy(valueDeTimestampActualMayorTemporalesC, "");
						string_append(&valueDeTimestampActualMayorTemporalesC,
								vectorStructsTemporalC[0]->value);
					}
					fclose(archivoBloqueTmpC);
					free(pathBloqueTmpC);
					//free(vectorStructsTemporalC);
				} // cierra el for
			} // cierra el if
		} //cierra el while

		// si encontro alguno, me guarda el timestamp mayor en timestampActualMayorTemporalesC
		// y guarda el valor en valueDeTimestampActualMayorTemporalesC
		// si no hay ninguno en vectorStructsTemporalC
		// entonces timestampActualMayorTemporalesC = -1 y
		// valueDeTimestampActualMayorTemporalesC = NULL

		closedir(directorioTemporal);

		// ----------------------------------------------------

		// LEO MEMTABLE

		t_registro **entradaTabla = dictionary_get(memtable, tabla); //me devuelve un array de t_registro's

		int cantIgualDeKeyEnMemtable = 0;
		//creo nuevo array que va a tener solo los structs de la key que me pasaron por parametro
		t_registro* arrayPorKeyDeseadaMemtable[100];
		crearArrayPorKeyMemtable(arrayPorKeyDeseadaMemtable, entradaTabla,
				atoi(key), &cantIgualDeKeyEnMemtable);

		int t = 0;
		char* unValor;
		for (int k = 1; k < cantIgualDeKeyEnMemtable; k++) {
			for (int j = 0; j < (cantIgualDeKeyEnMemtable - k); j++) {
				if (arrayPorKeyDeseadaMemtable[j]->timestamp
						< arrayPorKeyDeseadaMemtable[j + 1]->timestamp) {
					t = arrayPorKeyDeseadaMemtable[j + 1]->timestamp;
					unValor = malloc(
							strlen(arrayPorKeyDeseadaMemtable[j + 1]->value));
					strcpy(unValor, arrayPorKeyDeseadaMemtable[j + 1]->value);

					arrayPorKeyDeseadaMemtable[j + 1]->timestamp =
							arrayPorKeyDeseadaMemtable[j]->timestamp;
					arrayPorKeyDeseadaMemtable[j + 1]->value =
							arrayPorKeyDeseadaMemtable[j]->value;

					arrayPorKeyDeseadaMemtable[j]->timestamp = t;
					arrayPorKeyDeseadaMemtable[j]->value = unValor;
				}
			}
		} // aca quedaria el array arrayPorKeyDeseadaMemtable ordenado por timestamp mayor

		int timestampMayorMemtable;
		if (cantIgualDeKeyEnMemtable == 0) {
			timestampMayorMemtable = -1;
		} else {
			timestampMayorMemtable = arrayPorKeyDeseadaMemtable[0]->timestamp;
		}

		//-----------------------------------------------------
		char *valueFinal = string_new();

		// si no existe la key, error
		if ((timestampActualMayorBloques == -1)
				&& (timestampActualMayorTemporales == -1)
				&& (timestampMayorMemtable == -1)
				&& (timestampActualMayorTemporalesC == -1)) {
			char* mensajeALogear = malloc(
					strlen("Error: no existe la key numero ") + strlen(key)
							+ 1);
			strcpy(mensajeALogear, "Error: no existe la key numero ");
			strcat(mensajeALogear, key);
			t_log* g_logger;
			g_logger = log_create(
					string_from_format("%serroresSelect.log",
							structConfiguracionLFS.PUNTO_MONTAJE), "LFS", 1,
					LOG_LEVEL_INFO);
			log_error(g_logger, mensajeALogear);
			log_destroy(g_logger);
			free(mensajeALogear);

			return NULL;
		} else { // o sea, si existe la key en algun lugar

			// si bloques tiene mayor timestamp que todos
			if ((timestampActualMayorBloques >= timestampActualMayorTemporales)
					&& (timestampActualMayorBloques
							>= timestampActualMayorTemporalesC)
					&& (timestampActualMayorBloques >= timestampMayorMemtable)) {
				printf("%s\n", valueDeTimestampActualMayorBloques);
				string_append(&valueFinal, valueDeTimestampActualMayorBloques);
			}

			// si tmp tiene mayor timestamp que todos
			if ((timestampActualMayorTemporales >= timestampActualMayorBloques)
					&& (timestampActualMayorTemporales
							>= timestampActualMayorTemporalesC)
					&& (timestampActualMayorTemporales >= timestampMayorMemtable)) {
				printf("%s\n", valueDeTimestampActualMayorTemporales);
				string_append(&valueFinal,
						valueDeTimestampActualMayorTemporales);
			}

			// si tmpc tiene mayor timestamp que todos
			if ((timestampActualMayorTemporalesC
					>= timestampActualMayorTemporales)
					&& (timestampActualMayorTemporalesC
							>= timestampActualMayorBloques)
					&& (timestampActualMayorTemporalesC
							>= timestampMayorMemtable)) {
				printf("%s\n", valueDeTimestampActualMayorTemporalesC);
				string_append(&valueFinal,
						valueDeTimestampActualMayorTemporalesC);
			}

			// si memtable tiene mayor timestamp que todos
			if ((timestampMayorMemtable >= timestampActualMayorBloques)
					&& (timestampMayorMemtable >= timestampActualMayorTemporales)
					&& (timestampMayorMemtable
							>= timestampActualMayorTemporalesC)) {
				printf("%s\n", arrayPorKeyDeseadaMemtable[0]->value);
				string_append(&valueFinal,
						arrayPorKeyDeseadaMemtable[0]->value);
			}

			return valueFinal;
			string_append(&valueFinal, "");
		}

		free(pathMetadata);
		free(pathParticionQueContieneKey);
		free(pathTemporales);
		free(pathTemporalesC);
		//free(pathTemporal);
		config_destroy(tamanioYBloques);
		config_destroy(metadata);

		// SI NO ENCUENTRA LA TABLA (lo de abajo)
	} else {
		char* mensajeALogear = malloc(
				strlen("Error: no existe una tabla con el nombre ")
						+ strlen(tabla) + 1);
		strcpy(mensajeALogear, "Error: no existe una tabla con el nombre ");
		strcat(mensajeALogear, tabla);
		t_log* g_logger;
		//Si uso LOG_LEVEL_ERROR no lo imprime ni lo escribe
		g_logger = log_create(
				string_from_format("%serroresSelect.log",
						structConfiguracionLFS.PUNTO_MONTAJE), "LFS", 1,
				LOG_LEVEL_INFO);
		log_error(g_logger, mensajeALogear);
		log_destroy(g_logger);
		free(mensajeALogear);
	}
	return NULL;
}

void obtenerDatosParaKeyDeseada(FILE *fp, int key, t_registro** vectorStructs,
		int *cant) {
	int i = 0;
	char * line = NULL;
	size_t len = 0;
	ssize_t read;

	while ((read = getline(&line, &len, fp)) != -1) {
		int keyLeida = atoi(string_split(line, ";")[1]);
		if (keyLeida == key) {
			t_registro* p_registro = malloc(12); // 2 int = 2* 4        +       un puntero a char = 4
			t_registro p_registro2;
			p_registro = &p_registro2;
			char** arrayLinea = malloc(strlen(line) + 1);
			arrayLinea = string_split(line, ";");
			int timestamp = atoi(arrayLinea[0]);
			int key = atoi(arrayLinea[1]);
			p_registro->timestamp = timestamp;
			p_registro->key = key;
			p_registro->value = malloc(strlen(arrayLinea[2]));
			strcpy(p_registro->value, arrayLinea[2]);
			vectorStructs[i] = malloc(12);
			memcpy(&vectorStructs[i]->key, &p_registro->key,
					sizeof(p_registro->key));
			memcpy(&vectorStructs[i]->timestamp, &p_registro->timestamp,
					sizeof(p_registro->timestamp));
			vectorStructs[i]->value = malloc(strlen(p_registro->value));
			memcpy(vectorStructs[i]->value, p_registro->value,
					strlen(p_registro->value));
			i++;
			(*cant)++;
		}
	}
}

void crearArrayPorKeyMemtable(t_registro** arrayPorKeyDeseadaMemtable,
		t_registro **entradaTabla, int laKey, int *cant) {
	for (int i = 0; i < 100; i++) {
		if (entradaTabla[i]->key == laKey) {
			arrayPorKeyDeseadaMemtable[*cant] = malloc(12);
			memcpy(&arrayPorKeyDeseadaMemtable[*cant]->key,
					&entradaTabla[i]->key, sizeof(entradaTabla[i]->key));
			memcpy(&arrayPorKeyDeseadaMemtable[*cant]->timestamp,
					&entradaTabla[i]->timestamp,
					sizeof(entradaTabla[i]->timestamp));

			arrayPorKeyDeseadaMemtable[*cant]->value = malloc(
					strlen(entradaTabla[i]->value));
			memcpy(arrayPorKeyDeseadaMemtable[*cant]->value,
					entradaTabla[i]->value, strlen(entradaTabla[i]->value));

			(*cant)++;
		}
	}
}

int estaEntreComillas(char* valor) {
	return string_starts_with(valor, "\"") && string_ends_with(valor, "\"");
}

metadataTabla describeUnaTabla(char *tabla) {
	char* pathTabla = string_new();
	string_append(&pathTabla,
			string_from_format("%sTables",
					structConfiguracionLFS.PUNTO_MONTAJE));
	string_append(&pathTabla, "/");
	string_append(&pathTabla, tabla);
	DIR *directorio = opendir(pathTabla);
	struct dirent *directorioALeer;
	while ((directorioALeer = readdir(directorio)) != NULL) {
		//Busco la metadata en la tabla
		if (!((directorioALeer->d_type) == DT_DIR) && !strcmp((directorioALeer->d_name), "metadata")) {
			char *pathMetadata = string_new();
			string_append(&pathMetadata, pathTabla);
			string_append(&pathMetadata, "/");
			string_append(&pathMetadata, directorioALeer->d_name);
			//Lleno el struct con los valores de la metadata
			metadataTabla metadataTabla;
			metadataTabla.CONSISTENCY = string_new();
			t_config *metadata = config_create(pathMetadata);
			string_append(&(metadataTabla.CONSISTENCY), config_get_string_value(metadata, "CONSISTENCY"));
			metadataTabla.PARTITIONS = config_get_int_value(metadata, "PARTITIONS");
			metadataTabla.COMPACTION_TIME = config_get_int_value(metadata, "COMPACTION_TIME");

			closedir(directorio);
			config_destroy(metadata);

			//Imprimo (si me lo pide memoria lo imprimo igual?)
			printf("%s: \n", tabla);
			printf("Particiones: %i\n", metadataTabla.PARTITIONS);
			printf("Consistencia: %s\n", metadataTabla.CONSISTENCY);
			printf("Tiempo de compactacion: %i\n\n", metadataTabla.COMPACTION_TIME);
			return metadataTabla;
		}
	}
	printf("¡Error! no se encontro la metadata");
	closedir(directorio);
	exit(-1);
}

t_dictionary *describeTodasLasTablas() {
	//Podria conservar la estructura en vez de borrar lo que tengo adentro pero tendria que ir fijandome que los valores que
	//tenga adentro el diccionario todavia sean validos, entonces es mas facil borrar todo y hacer describe desde 0
	dictionary_clean(diccionarioDescribe);

	DIR *directorio = opendir(string_from_format("%sTables", structConfiguracionLFS.PUNTO_MONTAJE));
	struct dirent *directorioALeer;
	while ((directorioALeer = readdir(directorio)) != NULL) {
		//Busco la metadata de todas las tablas (evaluo que no ingrese a los directorios "." y ".."
		if ((directorioALeer->d_type) == DT_DIR && strcmp((directorioALeer->d_name), ".") && strcmp((directorioALeer->d_name), "..")) {
			metadataTabla structMetadata;
			structMetadata = describeUnaTabla(directorioALeer->d_name);
			dictionary_put(diccionarioDescribe, directorioALeer->d_name, &structMetadata);

			/* Para probar que funciona esta wea
			 *
			 * metadataTabla *metadata;
			 * metadata = (metadataTabla *)dictionary_get(diccionarioDescribe, directorioALeer->d_name);
			 * printf("%i", metadata->COMPACTION_TIME);
			 *
			*/
		}
	}
	closedir(directorio);
	return diccionarioDescribe;

}


/*

 void iniciarConexion() {
 int opt = TRUE;
 int master_socket, addrlen, new_socket, client_socket[30], max_clients = 30,
 activity, i, valread, sd;
 int max_sd;
 struct sockaddr_in address;

 char buffer[1025]; //data buffer of 1K

 //set of socket descriptors
 fd_set readfds;

 //a message
 char *message = "Este es el mensaje del server\r\n";

 //initialise all client_socket[] to 0 so not checked
 for (i = 0; i < max_clients; i++) {
 client_socket[i] = 0;
 }

 //create a master socket
 if ((master_socket = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
 perror("socket failed");
 exit(EXIT_FAILURE);
 }

 //set master socket to allow multiple connections ,
 //this is just a good habit, it will work without this
 if (setsockopt(master_socket, SOL_SOCKET, SO_REUSEADDR, (char *) &opt,
 sizeof(opt)) < 0) {
 perror("setsockopt");
 exit(EXIT_FAILURE);
 }

 //type of socket created
 address.sin_family = AF_INET;
 address.sin_addr.s_addr = INADDR_ANY;
 address.sin_port = htons( structConfiguracionLFS.PUERTO_ESCUCHA);

 //bind the socket to localhost port 8888
 if (bind(master_socket, (struct sockaddr *) &address, sizeof(address))
 < 0) {
 perror("bind failed en lfs");
 exit(EXIT_FAILURE);
 }
 printf("Escuchando en el puerto: %d \n",  structConfiguracionLFS.PUERTO_ESCUCHA);

 listen(master_socket, 100);

 //accept the incoming connection
 addrlen = sizeof(address);
 puts("Esperando conexiones ...");

 while (TRUE) {
 //clear the socket set
 FD_ZERO(&readfds);

 //add master socket to set
 FD_SET(master_socket, &readfds);
 max_sd = master_socket;

 //add child sockets to set
 for (i = 0; i < max_clients; i++) {
 //socket descriptor
 sd = client_socket[i];

 //if valid socket descriptor then add to read list
 if (sd > 0)
 FD_SET(sd, &readfds);

 //highest file descriptor number, need it for the select function
 if (sd > max_sd)
 max_sd = sd;
 }

 //wait for an activity on one of the sockets , timeout is NULL ,
 //so wait indefinitely
 activity = select(max_sd + 1, &readfds, NULL, NULL, NULL);

 if ((activity < 0) && (errno != EINTR)) {
 printf("select error");
 }

 //If something happened on the master socket ,
 //then its an incoming connection
 if (FD_ISSET(master_socket, &readfds)) {
 new_socket = accept(master_socket, (struct sockaddr *) &address,
 (socklen_t*) &addrlen);
 if (new_socket < 0) {

 perror("accept");
 exit(EXIT_FAILURE);
 }

 //inform user of socket number - used in send and receive commands
 printf(
 "Nueva Conexion , socket fd: %d , ip: %s , puerto: %d 	\n",
 new_socket, inet_ntoa(address.sin_addr),
 ntohs(address.sin_port));

 //send new connection greeting message
 if (send(new_socket, structConfiguracionLFS.TAMANIO_VALUE,
 strlen(structConfiguracionLFS.TAMANIO_VALUE), 0)
 != strlen(structConfiguracionLFS.TAMANIO_VALUE)) {
 perror("send");
 }

 //puts("Welcome message sent successfully");

 //add new socket to array of sockets
 for (i = 0; i < max_clients; i++) {
 //if position is empty
 if (client_socket[i] == 0) {
 client_socket[i] = new_socket;
 printf("Agregado a la lista de sockets como: %d\n", i);

 break;
 }
 }
 }

 //else its some IO operation on some other socket
 for (i = 0; i < max_clients; i++) {
 sd = client_socket[i];

 if (FD_ISSET(sd, &readfds)) {
 //Check if it was for closing , and also read the
 //incoming message
 char *operacion = malloc(sizeof(int));
 valread = read(sd, operacion, sizeof(int));

 switch (atoi(operacion)) {
 case 1:
 //Select
 tomarPeticionSelect(sd);
 break;

 case 3:
 //Create
 tomarPeticionCreate(sd);
 break;

 case 6:
 //Insert
 tomarPeticionInsert(sd);
 break;

 default:
 break;

 }

 if (valread == 0) {
 //Somebody disconnected , get his details and print
 getpeername(sd, (struct sockaddr*) &address,
 (socklen_t*) &addrlen);
 printf("Host disconnected , ip %s , port %d \n",
 inet_ntoa(address.sin_addr),
 ntohs(address.sin_port));

 //Close the socket and mark as 0 in list for reuse
 close(sd);
 client_socket[i] = 0;
 }

 Echo back the message that came in
 else {
 //set the string terminating NULL byte on the end
 //of the data read
 char mensaje[] = "Le llego tu mensaje al File System";
 buffer[valread] = '\0';
 printf("Memoria %d: %s\n", sd, buffer);
 send(sd, mensaje, strlen(mensaje), 0);

 }
 }
 }
 }

 }
 */

/*
 void tomarPeticionSelect(int sd) {
 char *tamanioTabla = malloc(sizeof(int));
 read(sd, tamanioTabla, sizeof(int));
 char *tabla = malloc(atoi(tamanioTabla));
 read(sd, tabla, tamanioTabla);
 char *tamanioKey = malloc(sizeof(int));
 read(sd, tamanioKey, sizeof(int));
 char *key = malloc(atoi(tamanioKey));
 read(sd, key, tamanioKey);
 printf("Haciendo Select");
 char *value = realizarSelect(tabla, key);
 send(sd, value, structConfiguracionLFS.TAMANIO_VALUE, 0);
 }

 void tomarPeticionCreate(int sd) {
 char *tamanioTabla = malloc(sizeof(int));
 read(sd, tamanioTabla, sizeof(int));
 char *tabla = malloc(atoi(tamanioTabla));
 read(sd, tabla, tamanioTabla);
 char *tamanioConsistencia = malloc(sizeof(int));
 read(sd, tamanioConsistencia, sizeof(int));
 char *tipoConsistencia = malloc(atoi(tamanioConsistencia));
 read(sd, tipoConsistencia, tamanioConsistencia);
 char *tamanioNumeroParticiones = malloc(sizeof(int));
 read(sd, tamanioNumeroParticiones, sizeof(int));
 char *numeroParticiones = malloc(tamanioNumeroParticiones);
 read(sd, numeroParticiones, tamanioNumeroParticiones);
 char *tamanioTiempoCompactacion = malloc(sizeof(int));
 read(sd, tamanioTiempoCompactacion, sizeof(int));
 char *tiempoCompactacion = malloc(tamanioTiempoCompactacion);
 read(sd, tiempoCompactacion, tamanioTiempoCompactacion);
 create(tabla, tipoConsistencia, numeroParticiones, tiempoCompactacion);
 }

 void tomarPeticionInsert(int sd) {
 char *tamanioTabla = malloc(sizeof(int));
 read(sd, tamanioTabla, sizeof(int));
 char *tabla = malloc(atoi(tamanioTabla));
 read(sd, tabla, tamanioTabla);

 char *tamanioKey = malloc(sizeof(int));
 read(sd, tamanioKey, sizeof(int));
 char *key = malloc(atoi(tamanioKey));
 read(sd, key, tamanioKey);
 char *tamanioValue = malloc(sizeof(int));
 read(sd, tamanioValue, sizeof(int));
 char *value = malloc(tamanioValue);
 read(sd, value, tamanioValue);

 //insert(tabla, key, value);
 printf("Haciendo insert");
 }

 */
