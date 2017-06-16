import rethinkdb as r

CAMPEONATOS = "campeonatos"
ARBITROS = "arbitros"
COMPETIDORES =  "competidores"
ESCUELAS = "escuelas"
MODALIDADES = "modalidades"
CATEGORIAS = "categorias"
NOMBREESCUELA = "nombreEscuela"

def connectToDB():
    r.connect().repl()

def createTables():
    r.table_create(CAMPEONATOS, primary_key = "ano").run()
    r.table_create(ARBITROS, primary_key = "placaArbitro").run()
    r.table_create(COMPETIDORES, primary_key = "dniCompetidor").run()
    r.table_create(ESCUELAS, primary_key = "nombre").run()
    r.table_create(MODALIDADES, primary_key = "nombre").run()
    r.table_create(CATEGORIAS, primary_key = "id").run()
    r.table_create(NOMBREESCUELA, primary_key = "nombre").run()

### Insertions

def insertCampeonato(ano):
    campeonato = {"ano" : ano,
                  "competidores" : dict(),
                  "categorias" : [],
                  "arbitros" : [] }
    r.table(CAMPEONATOS).insert(campeonato).run()

def insertModalidad(nombre):
    modalidad = { "nombre": nombre,
                  "record" : 0,
                  "holders" : [] }
    r.table(MODALIDADES).insert(modalidad).run()

def crearCompetidor(dniCompetidor, nombre, nombreEscuela):
    competidor = { "dniCompetidor": dniCompetidor,
                   "nombre" : nombre,
                   "escuela" : nombreEscuela,
                   "medallas" : dict()}
    ##ya debe existir la escuela
    r.table(COMPETIDORES).insert(competidor).run()

def insertCompetidor(anoCampeonato, dniCompetidor):
    ##ya debe existir
    escuela = r.table(COMPETIDORES).get(dniCompetidor).run()["escuela"]
    dic = r.table(CAMPEONATOS).get(anoCampeonato).run()["competidores"]
    dic[str(dniCompetidor)] = (escuela, 0)
    r.table(CAMPEONATOS).get(anoCampeonato).update({"competidores" : dic}).run()

def insertEscuela(nombre, pais):
    escuela = { "nombre": nombre,
                "campeonato": dict(),
                "competidores" : [],
                "pais" : pais}
    r.table(ESCUELAS).insert(escuela).run()

def insertCategoria(anoCampeonato, nombreModalidad, pesoMin, pesoMax, edadMin, edadMax, genero, graduacion):
    ##La modalidad debe existir
    categoria = {
            "id" : str(anoCampeonato) + ":" + str(nombreModalidad) + ":" + str(pesoMin) + ":" + str(pesoMax) + ":" + str(edadMin) + ":" + str(edadMax) + ":" + str(genero) + ":" + str(graduacion),
            "partidos": [],
            "peso" : {"min" : pesoMin, "max" : pesoMax},
            "edad" : {"min" : edadMin, "max" : edadMax},
            "genero" : genero,
            "graduacion" : graduacion,
            "medallero": {"oro":{}, "plata":{}, "bronce":{}},
            "modalidad": nombreModalidad }
    r.table(CATEGORIAS).insert(categoria).run()

def insertArbitro(placaArbitro, nombre):
    arbitro = {
            "placaArbitro" : placaArbitro,
            "nombre" : nombre,
            "categorias" : []}
    r.table(ARBITROS).insert(arbitro).run()

def insertPartido(categoria, dniGanador, dniPerdedor, placaArbitro):
    ## Deben existir los 4

    ### Actualizar categoria

    IDcategoria = getCategoria(categoria)

    array = r.table(CATEGORIAS).get(IDcategoria).run()["partidos"]
    array += [{"dniGanador" : dniGanador, "dniPerdedor" : dniPerdedor, "placaArbitro": placaArbitro}]
    r.table(CATEGORIAS).get(IDcategoria).update({"partidos" : array }).run()

    ### Actualizar campeonato

    dic = r.table(CAMPEONATOS).get(categoria.get("anoCampeonato")).get_field("competidores").run()
    dic[str(dniGanador)][1] += 1
    r.table(CAMPEONATOS).get(categoria.get("anoCampeonato")).update({"competidores" : dic}).run()

    ### Actualizar arbitro

    array = r.table(ARBITROS).get(placaArbitro).get_field("categorias").run()
    array += {"id" : IDcategoria}
    r.table(ARBITROS).get(placaArbitro).update({"categorias" : array}).run()

def insertMedalla(categoria, dniCompetidor, tipoMedalla):

    anoCampeonato = categoria["anoCampeonato"]
    IDcategoria = getCategoria(categoria)

    modalidad = r.table(CATEGORIAS).get(IDcategoria).run()["modalidad"]
    ## Debe competir en esa categoria ese ano

    ### Actualizar record modalidad
    record = r.table(MODALIDADES).get(modalidad).run()["record"]
    medallasCompetidor = r.table(COMPETIDORES).get(dniCompetidor).get_field("medallas").run()[modalidad]
    if (record == medallasCompetidor):
        record += 1
        r.table(MODALIDADES).get(modalidad).update({"record" : record, "holders" : [dniCompetidor]}).run()
    elif (record == medallasCompetidor + 1):
        holders = r.table(MODALIDADES).get(modalidad).run()["holders"]
        r.table(MODALIDADES).get(modalidad).update({"record" : record, "holders" : holders + [dniCompetidor]}).run()

    ### Actualizar competidor

    dic = r.table("competidor").get(dniCompetidor).get_field("medallas").run()
    dic[modalidad] += 1
    r.table(COMPETIDORES).get(dniCompetidor).update({"medallas" : dic}).run()

    ### Actualizar escuela

    escuela = r.table(COMPETIDORES).get(dniCompetidor).get_field("escuela").run()
    dic = r.table(ESCUELAS).get(escuela).run()["campeonato"]
    dic[anoCampeonato] += 1
    r.table(ESCUELAS).get(escuela).update({"campeonato": dic}).run()

    ### Actualizar categoria

    nombre = r.table(COMPETIDORES).get(dniCompetidor)
    dic = r.table(CATEGORIAS).get(IDcategoria).get_field("medallero").run()
    dic[tipoMedalla] = {"dni" : dniCompetidor, "nombre" : nombre}
    dic = r.table(CATEGORIAS).get(IDcategoria).update({"medallero" : dic})

def getCategoria(categoria):
    ##La modalidad debe existir
    return str(categoria.get("anoCampeonato")) + ":" + str(categoria.get("nombreModalidad")) + ":" + str(categoria.get("pesoMin")) + ":" + str(categoria.get("pesoMin")) + ":" + str(categoria.get("edadMin")) + ":" + str(categoria.get("edadMax")) + ":" + str(categoria.get("genero")) + ":" + str(categoria.get("graduacion"))


### Queries

def PGxCompxCamp(dniCompetidor, anoCampeonato):
    competitors = r.table(CAMPEONATOS).get(anoCampeonato).get_field("competidores").run()
    return competitors.filter(lambda c: c["DNI"] == dniCompetidor).map(lambda c: c["PG"]).run()[0]

    ### Sacar partidos ganados

def medallasxEscuela(nombreEscuela):
    idEscuela = r.table(NOMBREESCUELA).get(nombreEscuela).run()
    return r.table(ESCUELAS).get(idEscuela).get_field("campeonatos").sum("medallas").run()

def mejorCampxEscuela(nombreEscuela):
    idEscuela = r.table(NOMBREESCUELA).get(nombreEscuela).run()
    return r.table(ESCUELAS).get(idEscuela).get_field("campeonatos").max(lambda c: c["medallas"]).run()["ano"]

def arbitrosMasde4Campeonatos():
    return r.table("arbitros").filter(lambda row: row["participaciones"] > 4).run()
    ### Ver el filtro

def escuelasConMasComps(anoCampeonato):
    competitors = r.table(CAMPEONATOS).get(anoCampeonato).get_field("competidores").run()
    schools = competitors.group(lambda c: c["escuela"]["Nombre"]).count().ungroup().map(lambda group: {
        "value": group["reduction"],
        "school": group["group"]
        }).group(lambda x: x["value"]).run()
    most_competitors = schools.max(schools.keys()).run()
    return schools[most_competitors]

def escuelasConMasCompsMapReduce(anoCampeonato):
    competitors = r.table(CAMPEONATOS).get(anoCampeonato).get_field("competidores").run()
    schools = competitors.map(lambda c: {
        "school": c["escuela"]["Nombre"],
    "value": 1
    }).group("school").reduce(lambda a, b: {
        "school": a["school"],
        "count": a["count"] + b["count"]
    }).ungroup().map(lambda g: {"value": group["reduction"],
        "school": group["group"]
        }).group(lambda x: x["value"]).run()
    most_competitors = schools.max(schools.keys()).run()
    return schools[most_competitors]


def competidoresMasMedallasxMod(nombreModalidad): #Si es 0 no devuelve nada
    return r.table(MODALIDADES).get(nombreModalidad).get_field("Holders").map(lambda c: c["Nombre"]).run()

if __name__ == '__main__':
    connectToDB()
    #createTables()
    '''for i in range (6):
        insertModalidad("modalidad" + str(i))
    for i in range (13):
        insertEscuela("escuela" + str(i), "pais"+str(i))'''
    '''for i in range (1, 26):
        crearCompetidor(10000000 + i, "competidor" + str(i), "escuela" + str(i/2))'''
    '''for i in range (2000, 2017):
        print i
#        insertCampeonato(i)
        for j in range (1, 26):
            insertCompetidor(i, 10000000 + j)'''

#    insertCategoria(2002, None, None, None, None, None, None, None)

#    insertArbitro(315, "Eliz hondo")

#    insertCategoria(2002, "A", 10, 20, 30, 40, "Apache", 1)

#    insertPartido({"anoCampeonato" : 2002}, 10000001, 10000002, 314)
#    print getCategoria({"anoCampeonato" : 2002})
#    for i in r.table(CAMPEONATOS).run().items: 
#        if i["ano"] == 2002 : print i["competidores"]

#    print r.table(MODALIDADES).get("modalidad1").run()
    insertCategoria(2002, "modalidad1", None, None, None, None, None, None)
    insertMedalla({"anoCampeonato" : 2002, "nombreModalidad" : "modalidad1"}, 10000001, "oro")

