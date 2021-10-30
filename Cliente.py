import socket, pickle, struct,os, colorama

class Cliente():
    
    def __init__(self):
        self.hostname = '172.31.62.214'
        self.port = 5050 
        self.dicc = {}
     
    def iniciar_conexion(self):
        # Iniciar servicio
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.connect((self.hostname, self.port))
        except Exception as e:
            print(e)
    
    def leer_comando(self):
        x = input('ingrese comando')
        self.dicc[1] = x
    
    def leer_archivo(self, path):
        file = open(path, 'rb')
        self.dicc['contenido'] = file.read()
        file.read()
        self.dicc['nombreArchivo'] = os.path.split(path)[1]
        file.close()
    
    def enviar_archivo(self):
        # Enviar datos al servidor
        # print("pasa por aca ")
        x = pickle.dumps(self.dicc)
        length = len(x)
        self.sock.sendall(struct.pack('!I', length))
        self.sock.sendall(x)
        # print(self.recvall(1024))
        # print(respuesta)
       
    def recvall (self):
        msg = self.sock.recv(17520)
        msgg = pickle.loads(msg)
        print(colorama.Back.BLUE+colorama.Fore.RED+ msgg['mensaje']+colorama.Style.RESET_ALL)
        if(msgg['archivo'] != "no"):
            f = open(msgg['nombreArchivo'],'wb')
            f.write(msgg['archivo'])
            f.close()

    def recvallfile(self):
        
        msg = self.sock.recv(17520)
        msgg = pickle.loads(msg)
        print(colorama.Back.BLUE+colorama.Fore.RED+ msgg['mensaje']+colorama.Style.RESET_ALL)
        if(msgg['mensaje'] != ( f"No existe el bucket {self.dicc['bucket']}")):
            print(msgg['lista'])
            for fichero in msgg['lista']:
                print("- " + fichero)    

    def cerrar_conexion(self):
        self.sock.close()
                
    def inter_user(self):
        variable = 1;
        colorama.init()
        while(int(variable) < 9):
            print('\n\nBienvenido a la central de control servidor-socket. En que le podemos ayudar hoy?''\n'+ '1)Crear Key''\n' +'2)Eliminar una Key''\n' +'3)Ver lista de todos los buckets'+'\n4)Eliminar un archivo de una Key \n5)Listar archivos de una Key\n6)Validar carpetas en servidor\n7)Validar carpetas en nodos\n8)Acabar con la conexion')
            variable= input()
            if(variable == '1'):
                self.dicc['comando'] = '1'
                self.dicc['bucket'] = input('Ingrese el nombre de la nueva key \n\n')
                path = input('Ingrese el path del archivo a subir \n\n')
                self.leer_archivo(path)
                self.enviar_archivo()
                self.recvall()
            if(variable == '2'):
                self.dicc['comando'] = '2'
                self.dicc['bucket'] = input('Ingrese el nombre de la key a eliminar\n\n')
                self.enviar_archivo()
                self.recvall()
            if(variable == '3'):
                self.dicc['comando'] = '3'
                self.dicc['bucket'] = " "
                self.enviar_archivo()
                self.recvallfile()
           
            if(variable == '4'):
                self.dicc['comando'] = '4'
                bucket = input('Ingrese el key de donde quiere eliminar el archivo \n\n')
                path = input('Ingrese el nombre del archivo a eliminar (con extension) \n\n')
                self.dicc['bucket'] = bucket
                self.dicc['nombreArchivo'] = path
                self.enviar_archivo()
                self.recvall()
            if(variable == '5'):
                self.dicc['comando'] = '5'
                self.dicc['bucket'] = input("Ingrese el valor de la key para listar sus archivos\n\n")
                self.enviar_archivo()
                self.recvallfile()
            if(variable == '6'):
                self.dicc['comando'] = '6'
                self.enviar_archivo()
                self.recvall()
            if(variable == '7'):
                self.dicc['comando'] = '7'
                self.enviar_archivo()
                self.recvall()
            if(variable == '8'):
                self.dicc['comando'] = '8'
                self.enviar_archivo()
                self.cerrar_conexion()
                self.sock.close()
                break

if __name__ == "__main__":
    c = Cliente()
    c.iniciar_conexion()
    c.inter_user()
    
