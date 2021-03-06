import rethinkdb as r

CAMPEONATOS = "campeonatos"
ARBITROS = "arbitros"
COMPETIDORES = "competidores"
ESCUELAS = "escuelas"
MODALIDADES = "modalidades"
CATEGORIAS = "categorias"
NOMBREESCUELA = "nombreEscuela"


def connectToDB():
    r.connect().repl()


def createTables():
    r.table_create(CAMPEONATOS, primary_key="ano").run()
    r.table_create(ARBITROS, primary_key="placaArbitro").run()
    r.table_create(COMPETIDORES, primary_key="dniCompetidor").run()
    r.table_create(ESCUELAS, primary_key="nombre").run()
    r.table_create(MODALIDADES, primary_key="nombre").run()
    r.table_create(CATEGORIAS, primary_key="id").run()
    r.table_create(NOMBREESCUELA, primary_key="nombre").run()


def deleteTables():
    r.table_drop(CAMPEONATOS).run()
    r.table_drop(ARBITROS).run()
    r.table_drop(COMPETIDORES).run()
    r.table_drop(ESCUELAS).run()
    r.table_drop(MODALIDADES).run()
    r.table_drop(CATEGORIAS).run()
    r.table_drop(NOMBREESCUELA).run()
# Insertions


def insertCampeonato(ano):
    campeonato = {"ano": ano,
                  "competidores": dict(),
                  "categorias": [],
                  "arbitros": []}
    r.table(CAMPEONATOS).insert(campeonato).run()


def insertModalidad(nombre):
    modalidad = {"nombre": nombre,
                 "record": 0,
                 "holders": []}
    r.table(MODALIDADES).insert(modalidad).run()


def crearCompetidor(dniCompetidor, nombre, nombreEscuela):
    competidor = {"dniCompetidor": dniCompetidor,
                  "nombre": nombre,
                  "escuela": nombreEscuela,
                  "medallas": dict()}
    # ya debe existir la escuela
    r.table(COMPETIDORES).insert(competidor).run()


def insertCompetidor(anoCampeonato, dniCompetidor):
    # ya debe existir
    escuela = r.table(COMPETIDORES).get(dniCompetidor).run()["escuela"]
    dic = r.table(CAMPEONATOS).get(anoCampeonato).run()["competidores"]
    dic[str(dniCompetidor)] = {"escuela": {
        "ID": escuela, "Nombre": escuela}, "PG": 0}
    r.table(CAMPEONATOS).get(anoCampeonato).update({"competidores": dic}).run()

    competidores = r.table(ESCUELAS).get(
        escuela).get_field("competidores").run()
    if {"DNI": dniCompetidor} not in competidores:
        competidores += [{"DNI": dniCompetidor}]
    r.table(ESCUELAS).get(escuela).update({"competidores": competidores}).run()


def insertEscuela(nombre, pais):
    escuela = {"nombre": nombre,
               "campeonatos": dict(),
               "competidores": [],
               "pais": pais}
    r.table(ESCUELAS).insert(escuela).run()


def insertCategoria(anoCampeonato, nombreModalidad, pesoMin, pesoMax, edadMin, edadMax, genero, graduacion):
    # La modalidad debe existir
    categoria = {
        "id": str(anoCampeonato) + ":" + str(nombreModalidad) + ":" + str(pesoMin) + ":" + str(pesoMax) + ":" + str(edadMin) + ":" + str(edadMax) + ":" + str(genero) + ":" + str(graduacion),
        "partidos": [],
        "peso": {"min": pesoMin, "max": pesoMax},
        "edad": {"min": edadMin, "max": edadMax},
        "genero": genero,
        "graduacion": graduacion,
        "medallero": {"oro": {}, "plata": {}, "bronce": {}},
        "modalidad": nombreModalidad}
    r.table(CATEGORIAS).insert(categoria).run()


def insertArbitro(placaArbitro, nombre):
    arbitro = {
        "placaArbitro": placaArbitro,
        "nombre": nombre,
        "categorias": []}
    r.table(ARBITROS).insert(arbitro).run()


def insertPartido(categoria, dniGanador, dniPerdedor, placaArbitro):
    # Deben existir los 4

    # Actualizar categoria

    IDcategoria = getCategoria(categoria)

    array = r.table(CATEGORIAS).get(IDcategoria).run()["partidos"]
    array += [{"dniGanador": dniGanador,
               "dniPerdedor": dniPerdedor, "placaArbitro": placaArbitro}]
    r.table(CATEGORIAS).get(IDcategoria).update({"partidos": array}).run()

    # Actualizar campeonato

    dic = r.table(CAMPEONATOS).get(categoria.get(
        "anoCampeonato")).get_field("competidores").run()
    dic[str(dniGanador)]["PG"] += 1
    r.table(CAMPEONATOS).get(categoria.get("anoCampeonato")
                             ).update({"competidores": dic}).run()

    # Actualizar arbitro

    arbitro_has_categoria = r.table(ARBITROS).get(placaArbitro).get_field("categorias").contains(IDcategoria).run()
    if not arbitro_has_categoria:
        r.table(ARBITROS).get(placaArbitro).update({"categorias": r.row["categorias"] + [IDcategoria]}).run()


def insertMedalla(categoria, dniCompetidor, tipoMedalla):

    anoCampeonato = categoria["anoCampeonato"]
    IDcategoria = getCategoria(categoria)

    modalidad = r.table(CATEGORIAS).get(IDcategoria).run()["modalidad"]
    # Debe competir en esa categoria ese ano

    # Actualizar record modalidad y competidor
    medallasModalidad = 0
    record = r.table(MODALIDADES).get(modalidad).run()["record"]
    medallasCompetidor, nombre = r.table(COMPETIDORES).get(
        dniCompetidor).pluck("medallas", "nombre").run().values()

    if modalidad in medallasCompetidor:
        medallasModalidad = medallasCompetidor[modalidad]
        medallasCompetidor[modalidad] += 1
    else:
        medallasCompetidor[modalidad] = 1

    if (record == medallasModalidad):
        record += 1
        r.table(MODALIDADES).get(modalidad).update(
            {"record": record, "holders": [{"DNI": dniCompetidor, "Nombre": nombre}]}).run()
    elif (record == medallasModalidad + 1):
        holders = r.table(MODALIDADES).get(modalidad).run()["holders"]
        r.table(MODALIDADES).get(modalidad).update(
            {"record": record, "holders": holders + [{"DNI": dniCompetidor, "Nombre": nombre}]}).run()

    r.table(COMPETIDORES).get(dniCompetidor).update(
        {"medallas": medallasCompetidor}).run()

    # Actualizar escuela

    escuela = r.table(COMPETIDORES).get(
        dniCompetidor).get_field("escuela").run()
    dic = r.table(ESCUELAS).get(escuela).run()
    dic = dic["campeonatos"]
    if anoCampeonato in dic:
        dic[str(anoCampeonato)] += 1
    else:
        dic[str(anoCampeonato)] = 1
    r.table(ESCUELAS).get(escuela).update({"campeonatos": dic}).run()

    # Actualizar categoria

    nombre = r.table(COMPETIDORES).get(dniCompetidor)
    dic = r.table(CATEGORIAS).get(IDcategoria).get_field("medallero").run()
    dic[tipoMedalla] = {"dni": dniCompetidor, "nombre": nombre}
    dic = r.table(CATEGORIAS).get(IDcategoria).update({"medallero": dic})


def getCategoria(categoria):
    # La modalidad debe existir
    return str(categoria.get("anoCampeonato")) + ":" + str(categoria.get("nombreModalidad")) + ":" + str(categoria.get("pesoMin")) + ":" + str(categoria.get("pesoMax")) + ":" + str(categoria.get("edadMin")) + ":" + str(categoria.get("edadMax")) + ":" + str(categoria.get("genero")) + ":" + str(categoria.get("graduacion"))


# Queries

def PGxCompxCamp(dniCompetidor, anoCampeonato):
    competitors = r.table(CAMPEONATOS).get(anoCampeonato).get_field(
        "competidores")[str(dniCompetidor)].run()
    return competitors["PG"]


def medallasxEscuela(nombreEscuela):
    return r.table(ESCUELAS).get(nombreEscuela).get_field("campeonatos").values().sum().run()


def mejorCampxEscuela(nombreEscuela):
    return r.table(ESCUELAS).get(nombreEscuela).get_field("campeonatos").coerce_to("array").max(lambda kv: kv["PG"]).run()[0]


def getAnoCategoria(idCategoria):
    return idCategoria.split(":")[0]

def arbitrosMasde4Campeonatos():
    return r.table(ARBITROS).filter(r.row["categorias"].map(getAnoCategoria).distinct().count() > 3)["placaArbitro"].run()

def escuelasConMasComps():
    return r.table(CAMPEONATOS).map(lambda c: [c["ano"], escuelasConMasCompsCampeonato(c["ano"])]).run()


def escuelasConMasCompsCampeonato(anoCampeonato):
    schools = r.table(CAMPEONATOS).get(anoCampeonato)[
        'competidores'].values()["escuela"].group("Nombre")
    return schools.count().ungroup().max("reduction")["group"]


def escuelasConMasCompsMapReduceTotal():
    return r.table(CAMPEONATOS).map(lambda c: [c["ano"], escuelasConMasCompsMapReduce(c["ano"])]).run()


def escuelasConMasCompsMapReduce(anoCampeonato):
    dni_competitors = r.table(CAMPEONATOS).get(
        anoCampeonato)["competidores"].keys()
    return r.table(COMPETIDORES).filter(lambda c:
                                        dni_competitors.contains(
                                            c["dniCompetidor"].coerce_to('string'))
                                        ).group("escuela").count().ungroup().max("reduction")["group"]


def competidoresMasMedallasxMod(nombreModalidad):  # Si es 0 no devuelve nada
    modalidad = r.table(MODALIDADES).get(nombreModalidad)
    return r.branch(modalidad, modalidad["holders"]["Nombre"], []).run()

if __name__ == '__main__':
    connectToDB()
    deleteTables()
    createTables()

    # para sharding
    # from random import randint
    # count = 10**5

    # while count > 0:
    #     i = randint(0, 10**6)
    #     crearCompetidor(i, "competidor" +
    #                     str(i), "escuela" + str(i // 2))
    #     count -= 1

    # r.table(COMPETIDORES).reconfigure(shards=3)
    # # ver de reconfigurar en el admin web q aca parece q no anda
    # count = 10**5

    # while count > 0:
    #     i = randint(0, 10**6)
    #     crearCompetidor(i, "competidor" +
    #                     str(i), "escuela" + str(i // 2))
    #     count -= 1



    for i in range(6):
        insertModalidad("modalidad" + str(i))
    for i in range(13):
        insertEscuela("escuela" + str(i), "pais" + str(i))
    for i in range(1, 26):
        crearCompetidor(10000000 + i, "competidor" +
                        str(i), "escuela" + str(i // 2))
    for i in range(2000, 2003):
        insertCampeonato(i)
        for j in range(1, 26):
            insertCompetidor(i, 10000000 + j)

        # print(PGxCompxCamp(10000001, 2002))

        # print(medallasxEscuela("escuela0.5"))

        # mejorCampxEscuela(nombreEscuela)

        # arbitrosMasde4Campeonatos()

        # escuelasConMasComps(anoCampeonato)

        # escuelasConMasCompsMapReduce(anoCampeonato)

        # competidoresMasMedallasxMod(nombreModalidad) #Si es 0 no devuelve
        # nada

        # insertCategoria(2002, None, None, None, None, None, None, None)

    insertArbitro(315, "Eliz hondo")

    #    insertPartido({"anoCampeonato" : 2002}, 10000001, 10000002, 314)
    #    print getCategoria({"anoCampeonato" : 2002})
    #    for i in r.table(CAMPEONATOS).run().items:
    #        if i["ano"] == 2002 : print i["competidores"]

    #    print r.table(MODALIDADES).get("modalidad1").run()
    anoCampeonato = 2002
    nombreModalidad = "modalidad0"
    pesoMin = 10
    pesoMax = 20
    edadMin = 30
    edadMax = 40
    genero = "M"
    graduacion = 1

    insertCategoria(anoCampeonato, nombreModalidad, pesoMin,
                    pesoMax, edadMin, edadMax, genero, graduacion,)
    categoria = {
        "anoCampeonato": anoCampeonato,
        "pesoMin": pesoMin,
        "pesoMax": pesoMax,
        "edadMin": edadMin,
        "edadMax": edadMax,
        "genero": genero,
        "graduacion": graduacion,
        "nombreModalidad": nombreModalidad
    }

    insertMedalla(categoria, 10000001, "oro")

    insertPartido(categoria, 10000001, 10000002, 315)
    insertPartido(categoria, 10000002, 10000003, 315)
    insertPartido(categoria, 10000003, 10000004, 315)

    crearCompetidor(4, "nadie", "escuela1")
    insertCompetidor(2002, 4)

    # print(arbitrosMasde4Campeonatos())
    # print(competidoresMasMedallasxMod(nombreModalidad))

    # print(escuelasConMasCompsMapReduce(2002))
    # print(escuelasConMasComps())
    print(escuelasConMasCompsMapReduceTotal())

    # print r.table(COMPETIDORES).get(10000001).run()

    # print(PGxCompxCamp(10000001, 2002))
    # print(medallasxEscuela("escuela0"))
    # print(mejorCampxEscuela("escuela0"))
