
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
    r.table_create(COMPETIDORES, primary_key = "placaCompetidor").run()
    r.table_create(ESCUELAS, primary_key = "nombre").run()
    r.table_create(MODALIDADES, primary_key = "nombre").run()
    r.table_create(CATEGORIAS, primary_key = "nombre").run()
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

def crearCompetidor(placaCompetidor, nombre, nombreEscuela):
    competidor = { "placaCompetidor": placaCompetidor,
                   "escuela" : nombreEscuela,
                   "medallas" : dict()}
    ##ya debe existir la escuela
    r.table(COMPETIDORES).insert(competidor).run()

def insertCompetidor(anoCampeonato, placaCompetidor):
    ##ya debe existir
    escuela = r.table(COMPETIDORES).get(placaCompetidor).run()["escuela"]
    dic = r.table(CAMPEONATOS).get(anoCampeonato).run()["competidores"]
    dic[placaCompetidor] = (escuela, 0)
    r.table(CAMPEONATOS).get(anoCampeonato).update({"competidores" : dic}).run()

def insertEscuela(nombre):
    escuela = { "nombre": nombre,
                "campeonato": dict(),
                "competidores" : []}
    r.table(ESCUELAS).insert(escuela).run()

def insertCategoria(anoCampeonato, nombre, nombreModalidad):
    ##La modalidad debe existir
    categoria = {
            "id" : "c" + anoCampeonato + "m" + nombreModalidad + "c" + nombre,
            "partidos": [],
            "medallero": [], ###???
            "modalidad": nombreModalidad }
    r.table(CATEGORIAS).insert(categoria).run()

def insertArbitro(placaArbitro, nombre):
    arbitro = {
            "placaArbitro" : placaArbitro,
            "nombre" : nombre,
            "categorias" : []}
    r.table(ARBITROS).insert(arbitro).run()

def insertPartido(categoria, placaGanador, placaPerdedor, placaArbitro):
    ## Deben existir los 4

    ### Actualizar categoria

    array = r.table(CATEGORIAS).get(categoria).run()["partidos"]
    array += [(placaGanador, placaPerdedor, placaArbitro)]
    r.table(CATEGORIAS).get(categoria).update({"partidos" : array }).run()

    ### Actualizar campeonato

    # parametro o lo sacas de la categoria?
    dic = r.table(CAMPEONATOS).get(anoCampeonato).run()["competidores"]
    dic[placaGanador][1] += 1
    r.table(CAMPEONATOS).get(anoCampeonato).update({"competidores" : dic}).run()

def insertMedalla(anoCampeonato, categoria, placaCompetidor):
    modalidad = r.table(CATEGORIAS).get(categoria).run()["modalidad"]
    ## Debe competir en esa categoria ese ano

    ### Actualizar record modalidad
    record = r.table(MODALIDADES).get(modalidad).run()["record"]
    medallasCompetidor = r.table(COMPETIDORES).get(placaCompetidor).run()["medallas"] ### Corregir para que sea por modalidad
    if (record == medallasCompetidor):
        record += 1
        r.table(MODALIDADES).get(modalidad).update({"record" : record, "holders" : [placaCompetidor]}).run()
    elif (record == medallasCompetidor + 1):
        holders = r.table(MODALIDADES).get(modalidad).run()["holders"]
        r.table(MODALIDADES).get(modalidad).update({"record" : record, "holders" : holders + [placaCompetidor]}).run()

    ### Actualizar competidor

    dic = r.table("competidor").get(placaCompetidor).run()["medallas"]
    dic[modalidad] += 1
    r.table(COMPETIDORES).get(placaCompetidor).update({"medallas" : dic}).run()

    ### Actualizar escuela

    escuela = r.table(COMPETIDORES).get(placaCompetidor)["escuela"].run()
    dic = r.table(ESCUELAS).get(escuela).run()["campeonato"]
    dic[anoCampeonato] += 1
    r.table(ESCUELAS).get(escuela).update({"campeonato": dic}).run()

### Queries

def PGxCompxCamp(dniCompetidor, anoCampeonato):
    competitors = r.table(CAMPEONATOS).get(anoCampeonato).get_field("competidores").run()
    return competitors.filter(lambda c: c["DNI"] == dniCompetidor).run()

    ### Sacar partidos ganados

def medallasxEscuela(nombreEscuela):
    idEscuela = r.table(NOMBREESCUELA).get(nombreEscuela).run()
    return r.table(ESCUELAS).get(idEscuela).get_field("campeonatos").sum("medallas").run()

def mejorCampxEscuela(nombreEscuela):
    pass
        ### Max() devuelve toda la fila, pero no mas de una

def arbitrosMasde4Partip():
    return r.table("arbitros").filter(lambda row: row["participaciones"] > 4).run().items
    ### Ver el filtro

def escuelasConMasComps(anoCampeonato):
    pass

def competidoresMasMedallasxMod(nombreModalidad): #Si es 0 no devuelve nada
    pass

if __name__ == '__main__':
    connectToDB()
    createTables()
