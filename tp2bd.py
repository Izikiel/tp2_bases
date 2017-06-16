#!/usr/bin/2.7

import rethinkdb as r

def connectToDB():
    r.connect().repl()

def createTables():
    r.table_create("campeonatos", primary_key = "ano").run()
    r.table_create("arbitros", primary_key = "placaArbitro").run()
    r.table_create("competidores", primary_key = "placaCompetidor").run()
    r.table_create("escuelas", primary_key = "nombre").run()
    r.table_create("modalidades", primary_key = "nombre").run()
    r.table_create("categorias", primary_key = "nombre").run()

connectToDB()

### Insertions

def insertCampeonato(ano):
    campeonato = {"ano" : ano,
                  "competidores" : dict(),
                  "categorias" : [],
                  "arbitros" : [] }
    r.table("campeonatos").insert(campeonato).run()

def insertModalidad(nombre):
    modalidad = { "nombre": nombre,
                  "record" : 0,
                  "holders" : [] }
    r.table("modalidades").insert(modalidad).run()

def crearCompetidor(placaCompetidor, nombre, nombreEscuela):
    competidor = { "placaCompetidor": placaCompetidor,
                   "escuela" : nombreEscuela,
                   "medallas" : dict()}
    ##ya debe existir la escuela
    r.table("competidores").insert(competidor).run()
    
def insertCompetidor(anoCampeonato, placaCompetidor):
    ##ya debe existir
    escuela = r.table("competidores").get(placaCompetidor).run()["escuela"]
    dic = r.table("campeonatos").get(anoCampeonato).run()["competidores"]
    dic[placaCompetidor] = (escuela, 0)
    r.table("campeonatos").get(anoCampeonato).update({"competidores" : dic}).run()

def insertEscuela(nombre):
    escuela = { "nombre": nombre,
                "campeonato": dict(),
                "competidores" : []}
    r.table("escuelas").insert(escuela).run()

def insertCategoria(anoCampeonato, nombre, nombreModalidad):
    ##La modalidad debe existir
    categoria = {
            "id" : "c" + anoCampeonato + "m" + nombreModalidad + "c" + nombre,
            "partidos": [],
            "medallero": [], ###???
            "modalidad": nombreModalidad }
    r.table("categorias").insert(categoria).run()

def insertArbitro(placaArbitro, nombre):
    arbitro = {
            "placaArbitro" : placaArbitro,
            "nombre" : nombre,
            "categorias" : []}
    r.table("arbitros").insert(arbitro).run()

def insertPartido(categoria, placaGanador, placaPerdedor, placaArbitro):
    ## Deben existir los 4

    ### Actualizar categoria

    array = r.table("categorias").get(categoria).run()["partidos"]
    array += [(placaGanador, placaPerdedor, placaArbitro)]
    r.table("categorias").get(categoria).update({"partidos" : array }).run()

    ### Actualizar campeonato

    # parametro o lo sacas de la categoria?
    dic = r.table("campeonatos").get(anoCampeonato).run()["competidores"]
    dic[placaGanador][1] += 1
    r.table("campeonatos").get(anoCampeonato).update({"competidores" : dic}).run()

def insertMedalla(anoCampeonato, categoria, placaCompetidor):
    modalidad = r.table("categorias").get(categoria).run()["modalidad"]
    ## Debe competir en esa categoria ese ano

    ### Actualizar record modalidad
    record = r.table("modalidades").get(modalidad).run()["record"]
    medallasCompetidor = r.table("competidores").get(placaCompetidor).run()["medallas"] ### Corregir para que sea por modalidad
    if (record == medallasCompetidor):
        record += 1
        r.table("modalidades").get(modalidad).update({"record" : record, "holders" : [placaCompetidor]}).run()
    elif (record == medallasCompetidor + 1):
        holders = r.table("modalidades").get(modalidad).run()["holders"]
        r.table("modalidades").get(modalidad).update({"record" : record, "holders" : holders + [placaCompetidor]}).run()

    ### Actualizar competidor

    dic = r.table("competidor").get(placaCompetidor).run()["medallas"]
    dic[modalidad] += 1
    r.table("competidores").get(placaCompetidor).update({"medallas" : dic}).run()
        
    ### Actualizar escuela

    escuela = r.table("competidores").get(placaCompetidor)["escuela"].run()
    dic = r.table("escuela").get(escuela).run()["campeonato"]
    dic[anoCampeonato] += 1
    r.table("escuela").get(escuela).update({"campeonato": dic}).run()

### Queries

def PGxCompxCamp(placaCompetidor, anoCampeonato):
    r.table("campeonato").get(anoCampeonato).run()["competidores"]

    ### Sacar partidos ganados

def medallasxEscuela(nombreEscuela):
    dic = r.table("escuela").get(nombreEscuela).run()["campeonato"]
    return sum([dic[i] for i in dic])
    ### Creo que van a preferir que usemos agregacion y esas cosas que dejemos que python se encargue magicamente

def mejorCampxEscuela(nombreEscuela):
    print 0
    ### Max() devuelve toda la fila, pero no mas de una

def arbitrosMasde4Partip():
    return r.table("arbitros").filter(lambda row: row["participaciones"] > 4).run().items
    ### Ver el filtro

def escuelasConMasComps(anoCampeonato):
    print 0

def competidoresMasMedallasxMod(nombreModalidad): #Si es 0 no devuelve nada
    print 0

if __name__ == '__main__':
    connectToDB()
