# -*- coding: utf-8 -*-
from mrjob.job import MRJob
import time


tiempo_sesion = 36000
class Prac_logs_sesiones(MRJob):
    def mapper(self, _, line):
        line = line.split()
        line3 = line[3] #cogemos la seccion del tiempo
        data_time = line3[1:]
        pattern = '%d/%b/%Y:%H:%M:%S'
        epoch = time.mktime(time.strptime(data_time,pattern)) #https://docs.python.org/2/library/time.html#time.mktime
        yield line[0],(epoch,line[6]) #line[0] es la ip y la linea[6] es la web

    def reducer(self, key, emitido):
        lista_hora = []
        lista_pagina= []
        #recoge la hora y pagina para colocarlos en una lista
        for hora, pagina in emitido:
            lista_hora.append(hora)
            lista_pagina.append(pagina)

        #la lista que compara y guarda los indices, es decir, solo se queda con una sesion de la misma ip
        lista_indices =[]
        if len(lista_pagina)>1:
            i = 0
            while i<len(lista_pagina):
                j = i+1
                while j<len(lista_pagina):
                    #Borrar la repetecion de una misma sesion
                    if (lista_pagina[i]==lista_pagina[j]) and (lista_hora[j]-lista_hora[i]<tiempo_sesion):
                        print(" ")
                        lista_indices.append(i)
                    j = j+1
                i=i+1
        #filtro de las paginas y horas que se deben devolver
        for i in range(len(lista_indices)):
            lista_hora.pop(lista_indices[i])
            lista_pagina.pop(lista_indices[i])
        yield key,(lista_hora,lista_pagina)

if __name__ == '__main__':
    Prac_logs_sesiones.run()