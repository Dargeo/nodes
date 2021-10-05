import pickle, socket, multiprocessing, struct, os,shutil,filecmp
CHUNKSIZE = 1_000_000
class Server():

    def __init__(self, hostname, port):
        self.hostname = 'localhost'
        self.port = 5050
        self.dicc = {}
        self.connected = True

    def iniciar_conexion(self):
        # Iniciar servicio
        try:
            print('Escuchando')
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.bind((self.hostname, self.port))
            if(os.path.isdir('Buckets') == False):
                os.mkdir('Buckets')
            self.sock.listen(1)
        except Exception as e:
            print(e)

    def aceptar_conexion(self):
        # Aceptar solicitudes
        while True:
            self.conn, self.addr = self.sock.accept()
            print('conectado con %r', self.addr)
            # Manejo de procesos mediante el uso de un while y el manejo de un metodo
            proceso = multiprocessing.Process(target= self.recibir_datos, args=())
            # proceso.daemon = True
            proceso.start()
            
            print('Nuevo proceso inciado %r', proceso)

    def send_dir(self,soc):
        self.dicc['comando'] = '1'
        print(soc)
        self.env_dataNode(soc)
        slf = True
        while slf:
            
            
            
            print("Connected with node")
            
            with soc:
                for path,dirs,files in os.walk('Buckets'):
                    for file in files:
                        filename = os.path.join(path,file)
                        relpath = os.path.relpath(filename,'Buckets')
                        filesize = os.path.getsize(filename)

                        print(f'Sending {relpath}')

                        with open(filename,'rb') as f:
                            soc.sendall(relpath.encode() + b'\n')
                            soc.sendall(str(filesize).encode() + b'\n')

                            # Send the file in chunks so large files can be handled.
                            while True:
                                data = f.read(CHUNKSIZE)
                                if not data: break
                                soc.sendall(data)
                print('Done.')
                self.dicc['mensaje'] =f"Se esta ejecutando la validacion de carpetas"          
                self.enviar_archivo()
                slf = False
                
                 

    def recibir_data(self,soc):
        slf = True
        self.dicc['comando'] = '2'
        self.env_dataNode(soc)
        os.makedirs('Nodo',exist_ok=True)
        print("Searching for data")
        
        with soc,soc.makefile('rb') as clientfile:
            print("Entro 1")
            while True:
                
                
                raw = clientfile.readline()
                
                if not raw:break # no more files, server closed connection.

                filename = raw.strip().decode()
                
                length = int(clientfile.readline())
                print(f'Downloading {filename}...\n  Expecting {length:,} bytes...',end='')

                path = os.path.join('Nodo',filename)
                os.makedirs(os.path.dirname(path),exist_ok=True)

                # Read the data in chunks so it can handle large files.
                with open(path,'wb') as f:
                    while length:
                        chunk = min(length,CHUNKSIZE)
                        data = clientfile.read(chunk)
                        if not data:break
                        f.write(data)
                        length -= len(data)
                    else: # only runs if while doesn't break and length==0
                        print('Complete')
                        continue
                print('Incomplete')
                break 

        self.compare_folder_server()        
        self.dicc['mensaje'] =f"Se esta ejecutando la validacion de carpetas"          
        self.enviar_archivo()
        
            
            
    def crear_bucket(self,datos):

        if(os.path.isdir('Buckets/' + datos['bucket']) == False):
            self.dicc['mensaje'] = f" La carpeta {datos['bucket']} fue creada"

            os.mkdir('Buckets/' + datos['bucket'])
        else:
            self.dicc['mensaje'] =f"La carpeta {datos['bucket']} no fue creada, ya existe"
        self.enviar_archivo()

    #Eliminacion de buckets y archivos
    def eliminar_bucket(self,datos):
        nombre = 'Buckets/' + datos['bucket']
        shutil.rmtree(nombre)
        self.dicc['mensaje'] =f"el bucket {datos['bucket']} ha sido eliminado"
        self.enviar_archivo()


    def eliminar_archivo(self,datos):
        if(os.path.isfile('Buckets/' + datos['bucket'] +'/'+ datos['nombreArchivo'])== False):
            self.dicc['mensaje'] =f"El archivo {datos['nombreArchivo']} no existe"
        else:
            nombre = 'Buckets/' + datos['bucket']+'/' + datos['nombreArchivo']
            os.remove(nombre)
            self.dicc['mensaje'] =f"el archivo {datos['nombreArchivo']} ha sido eliminado"
        self.enviar_archivo()


    def enviar_archivo(self):
        message = pickle.dumps(self.dicc)
        self.conn.sendall(message)

    def env_dataNode(self,conn):
        # Enviar datos al servidor
        # print("pasa por aca ")
        x = pickle.dumps(self.dicc)
        length = len(x)
        conn.sendall(struct.pack('!I', length))
        conn.sendall(x)
        # print(self.recvall(1024))
        # print(respuesta)

    def recibir_datos(self):
        datos = ''
        while self.connected:
            try:
                # Recibir datos del cliente.
                lengthbuf = self.recvall(self.conn, 4)
                length, = struct.unpack('!I', lengthbuf)
                
                datos = self.recvall(self.conn, length)
                # self.conn.sendall(b'Se han recibido los datos')
                self.organizar_datos(datos)
            except Exception:
                self.connected = False

    def recvall (self, sock, count):
        buf = b''
        while count:
            newbuf = sock.recv (count)
            if not  newbuf: return None
            buf += newbuf
            count -= len (newbuf)
        return buf

    def compare_folder_server(self):
         print("entre")
         a = filecmp.dircmp('Buckets','Nodo').left_list
         b = filecmp.dircmp('Buckets','Nodo').right_list
         if len(a) != len(b):
             shutil.rmtree('Buckets')
             
             shutil.copytree('Nodo','Buckets')
    
    def guardar_archivo(self,datos):

        if(os.path.isdir('Buckets/' + datos['bucket']) == False):
            os.mkdir('Buckets/' + datos['bucket'])
        file = open('Buckets/' + datos['bucket']+'/'+datos['nombreArchivo'],'wb')
        file.write(datos['contenido'])
        file.close()
        self.dicc['mensaje'] =f"El archivo {datos['nombreArchivo']} ha sido guardado"
        self.enviar_archivo()

    def listar_archivos(self,datos):
        if(os.path.isdir('Buckets/' + datos['bucket']) == False):
            self.dicc['mensaje'] = f"No existe el bucket {datos['bucket']}"
        else:
            print(os.listdir('Buckets/' + datos['bucket']))
            self.dicc['lista'] = os.listdir('Buckets/' + datos['bucket'])
            print(self.dicc['lista'])
            self.dicc['mensaje'] = f"Esta es la lista de archivos dentro de {datos['bucket']}"
        self.enviar_archivo()

    def organizar_datos(self,x):
        print("Esperando comando")
        # swicth para llamara los metodos necesarios que envia el cliente
        self.dicc['lista'] = os.listdir('Buckets')
        self.dicc['mensaje'] = "Esta es la lista de buckets"
        self.dicc['archivo'] = "no"
        datos = pickle.loads(x)
        if(datos['comando'] == '1'):
            print("Recibi comando" + datos['comando'])
            self.guardar_archivo(datos)

        if(datos['comando'] == '2'):
            print("Recibi comando" + datos['comando'])
            self.eliminar_bucket(datos)

        if(datos['comando'] == '3'):
            print("Recibi comando" + datos['comando'])
            self.enviar_archivo()

        if(datos['comando'] == '4'):
            print("Recibi comando" + datos['comando'])
            self.eliminar_archivo(datos)
    
        if(datos['comando'] == '5'):
            print("Recibi comando" + datos['comando'])
            self.listar_archivos(datos)

        if(datos['comando'] == '6'):
            print("Recibi comando" + datos['comando'])
            self.conect_nodes(datos)

        if(datos['comando'] == '7'):
            print("Recibi comando" + datos['comando'])
            self.conect_nodes(datos)


        if(datos['comando'] == '8'):
            print("Recibi comando" + datos['comando'])
            print("Se esta cerrando la conexion con el cliente.")
            self.connected = False

    def cerrar_con(self,connect):
        # Cerrar conexión
        connect.close()



    def conect_nodes(self,datos):
        # Iniciar servicio
        try:
            self.sock2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock2.connect((self.hostname, 5000))
            print("Connected with%r",self.sock2)
            if datos['comando'] == '6':
                self.recibir_data(self.sock2)
            else:
                self.send_dir(self.sock2)
        except Exception as e:
            print(e)
            self.dicc['mensaje'] = "No se pudo establecer conexion con el Nodo"
            self.enviar_archivo()
        # try:
        #     self.sock3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #     self.sock3.connect((self.hostname, 8000))
        #     print("Connected with%r",self.sock2)
        #     if datos['comando'] == '6':
        #         self.recibir_data(self.sock3)
        #     else:
        #         self.send_dir(self.sock3)
        # except Exception as e:
        #     print(e)
            
if __name__ == "__main__":
 # Probar conexion entre cliente y socket
    s = Server( hostname = 'localhost', port = 5050)
    s.iniciar_conexion()
    s.aceptar_conexion()
    for proceso in multiprocessing.active_children():
        print('Terminando proceso %r', proceso)
        proceso.terminate()
        proceso.join()
    print('Listo')