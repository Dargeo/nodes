import pickle, socket, multiprocessing, struct, os,shutil,filecmp
CHUNKSIZE = 1_000_000
class Server():

    def __init__(self, hostname, port):
        self.hostname = 'localhost'
        self.port = 5000
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
            # self.client = self.sock.accept()
            print('conectado con %r', self.addr)
            # Manejo de procesos mediante el uso de un while y el manejo de un metodo
            self.connected = True
            self.recibir_datos()

    def send_dir(self):
        print("Entro al metodo para enviar")
        slf = True
        while slf:
            print("Entro al metodo para slf")
            
            with self.conn:
                for path,dirs,files in os.walk('Buckets'):
                    for file in files:
                        filename = os.path.join(path,file)
                        relpath = os.path.relpath(filename,'Buckets')
                        filesize = os.path.getsize(filename)

                        print(f'Sending {relpath}')

                        with open(filename,'rb') as f:
                            self.conn.sendall(relpath.encode() + b'\n')
                            self.conn.sendall(str(filesize).encode() + b'\n')

                            # Send the file in chunks so large files can be handled.
                            while True:
                                data = f.read(CHUNKSIZE)
                                print(data)
                                if not data: break
                                self.conn.sendall(data)
                print('Done.')
                slf=False
        print("Sali del while")
        self.conn.close()

    def recibir_data(self):
        os.makedirs('Servidor',exist_ok=True)
        
        
        datos = ''
        with self.conn,self.conn.makefile('rb') as clientfile:
            print("Searching for data")
            while True:
                
                raw = clientfile.readline()
                print(raw)
                if not raw: break # no more files, server closed connection.

                filename = raw.strip().decode()
                
                length = int(clientfile.readline())
                print(f'Downloading {filename}...\n  Expecting {length:,} bytes...',end='')

                path = os.path.join('Servidor',filename)
                os.makedirs(os.path.dirname(path),exist_ok=True)

                # Read the data in chunks so it can handle large files.
                with open(path,'wb') as f:
                    while length:
                        chunk = min(length,CHUNKSIZE)
                        data = clientfile.read(chunk)
                        if not data: print('no data')
                        f.write(data)
                        length -= len(data)
                    else: # only runs if while doesn't break and length==0
                        print('Complete')
                        continue
            self.compare_folder_server()

    def compare_folder_server(self):
         print("entre")
         a = filecmp.dircmp('Buckets','Servidor').left_list
         b = filecmp.dircmp('Buckets','Servidor').right_list
         if len(a) != len(b):
             shutil.rmtree('Buckets')
             
             shutil.copytree('Servidor','Buckets')

            
    def recibir_datos(self):
        datos = ''
        print(self.conn)
        
        while self.connected:
            try:
                # Recibir datos del cliente.
                lengthbuf = self.recvall(self.conn, 4)
                length, = struct.unpack('!I', lengthbuf)
                print("Estoy listo para recibir comandos...")
                datos = self.recvall(self.conn, length)
                # self.conn.sendall(b'Se han recibido los datos')
                
                self.organizar_datos(datos)
            except Exception:
                self.connected = False
                self.conn.close()
                print("Cerre conexion")
                print(self.conn)
                

    def recvall (self, sock, count):
        buf = b''
        while count:
            print(sock)
            newbuf = sock.recv (count)
            if not  newbuf: return None
            buf += newbuf
            count -= len (newbuf)
        return buf

    def organizar_datos(self,x):
        
        print("Esperando comando")
        # swicth para llamara los metodos necesarios que envia el cliente
        datos = pickle.loads(x)
        
        if(datos['comando'] == '1'):
            print("Recibido comando de recibir directorio")
            self.recibir_data()
            

        if(datos['comando'] == '2'):
            print("Recibido comando de enviar directorio")
            self.send_dir()
            


        if(datos['comando'] == '3'):
            print("Se esta cerrando el servidor.")
            self.connected = False

    def cerrar_con(self):
        # Cerrar conexión
        self.sock.close()

if __name__ == "__main__":
 # Probar conexion entre cliente y socket
    s = Server( hostname = 'localhost', port = 5000)
    s.iniciar_conexion()
    s.aceptar_conexion()
    
    print('Listo')