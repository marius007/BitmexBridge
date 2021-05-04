import win32pipe, win32file

class NamedPipe:
    def __init__(self, pipeName, enablePipe):
        self.status = 'init'
        self.pipeName =  '\\\\.\\pipe\\' + pipeName
        self.enablePipe = enablePipe
        if(self.enablePipe != True):
            return
        self.pipeHandler= win32pipe.CreateNamedPipe(self.pipeName,
                                                    win32pipe.PIPE_ACCESS_DUPLEX,
                                                    win32pipe.PIPE_TYPE_MESSAGE | win32pipe.PIPE_WAIT,
                                                    1, 65536, 65536,300,None)

    def Connect(self):
        if(self.enablePipe != True):
            return
        win32pipe.ConnectNamedPipe(self.pipeHandler, None)
        self.status = 'connected'

    def Disconnect(self):
        if(self.enablePipe != True):
            return
        win32pipe.DisconnectNamedPipe(self.pipeHandler)
        self.status = 'disconnected'

    def GetStatus(self):
        return self.status

    def Send(self,pstring):
        if(self.enablePipe != True):
            return
        try:
            message = bytes(pstring, 'utf-8')
            message_len= len(pstring).to_bytes(4, byteorder='little', signed=False)
            win32file.WriteFile(self.pipeHandler,  message_len + message)
        except Exception as e:
            exception_txt = str(e)
            print("--------Exception:" + exception_txt);
            if( "The pipe is being closed." in exception_txt ):
                self.status = 'disconnected'

    def Receive(self):
        result = ""
        if(self.enablePipe != True):
            return
        try:
            rc, data = win32file.ReadFile(self.pipeHandler, 4)
            size = int.from_bytes(data, byteorder='little')
            if(rc == 0):
                rc, data = win32file.ReadFile(self.pipeHandler, size)
                result = data.decode("utf-8")
            return result
        except Exception as e:
            exception_txt = str(e)
            print("--------Exception:" + exception_txt);
            if( "The pipe is being closed." in exception_txt ):
                self.status = 'disconnected'
            return result

