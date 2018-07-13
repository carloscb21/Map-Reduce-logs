# -*- coding: utf-8 -*-
"""
Created on Wed May 18 19:25:09 2016

@author: casa-pc
"""

from mrjob.job import MRJob
from mrjob.step import MRStep
import time


#devolvemos el numero de visitas para cada pagina por IP
tiempo_sesion = 36000 #segundos
class Prac_logs_comportamientos(MRJob):
    def mapper(self, _, line):
        line = line.split()
        line3 = line[3] #cogemos la seccion del tiempo
        data_time = line3[1:]
        pattern = '%d/%b/%Y:%H:%M:%S'
        epoch = time.mktime(time.strptime(data_time,pattern))#https://docs.python.org/2/library/time.html#time.mktime
        yield line[0],(epoch,line[6]) #line[0] es la ip y la linea[6] es la web

    #hacemos el mismo proceso que prac_logs.py
    def redu(self, key, datos):
        lista_hora = []
        lista_pagina= []
        lista_visitas = []
        for hora, pagina in datos:
            lista_hora.append(hora)
            lista_pagina.append(pagina)
            lista_visitas.append(1)
        lista_indices = []
        if len(lista_pagina)>1:
            i = 0
            while i<len(lista_pagina):
                j = i+1
                while j<len(lista_pagina):
                    if (lista_pagina[i]==lista_pagina[j]) and (lista_hora[j]-lista_hora[i]<tiempo_sesion):
                        lista_indices.append(i)
                    j = j+1
                i=i+1 
        for i in range(len(lista_indices)):
            lista_pagina.pop(lista_indices[i])
            lista_visitas.pop(1)
        for i in range(len(lista_pagina)):
            yield (key,lista_pagina[i]),lista_visitas[i]
    
    #mediante la clave sumamos las visitas
    def compor(self,key,valores):
        yield key,sum(valores)
        
    def steps(self):
        return [
            MRStep(mapper = self.mapper,
                    reducer = self.redu),
            MRStep(reducer = self.compor) 
        ]

if __name__ == '__main__':
    Prac_logs_comportamientos.run()