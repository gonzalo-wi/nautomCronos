import * as glide from "@glideapps/tables";
import dotenv from "dotenv";
import fs from "fs";
dotenv.config();

const ticketsTable = glide.table({
  token: process.env.GLIDE_TOKEN,
  app: process.env.GLIDE_APP,
  table: process.env.GLIDE_TABLE,
  columns: {
    idCreador: { type: "string", name: "ID" },
    novedad: { type: "string", name: "Novedad" },
    fechaCreaciN: { type: "date-time", name: "Fecha de creación" },
    idEmpleado: { type: "string", name: "Legajo creador" },
    fechaInicio: { type: "date-time", name: "Fecha de inicio" },
    fechaFin: { type: "date-time", name: "Fecha de fin" },
    comentario: { type: "string", name: "Comentario" },
    attachment: { type: "string", name: "Attachment" },
    validaciN: { type: "boolean", name: "Validación" },
    idValidador: { type: "string", name: "Validador" },
    lastModified: { type: "date-time", name: "59m64" },
    regiNAsignadaDesdeEjecuciN: { type: "string", name: "jOlLV" },
    eliminar: { type: "boolean", name: "6B3Yp" }
  }
});

function esFechaValida(fecha) {
  return typeof fecha === "string" && !isNaN(Date.parse(fecha));
}

export async function syncNovedad(novedad) {
  try {
    const registros = await ticketsTable.get();
    console.log("Registros obtenidos:", registros);

    if (!esFechaValida(novedad.fechaInicio) || !esFechaValida(novedad.fechaFin)) {
      console.error("🛑 Fecha inválida detectada:");
      console.error("fechaInicio:", novedad.fechaInicio);
      console.error("fechaFin:", novedad.fechaFin);
      throw new Error("Invalid fechaInicio o fechaFin en novedad");
    }

    const registroExistente = registros.find(
      (r) =>
        r.idEmpleado === novedad.idEmpleado &&
        r.novedad === novedad.novedad &&
        r.fechaInicio === new Date(novedad.fechaInicio).toISOString() &&
        r.fechaFin === new Date(novedad.fechaFin).toISOString()
    );

    if (registroExistente && registroExistente.$rowID) {
      await ticketsTable.update(registroExistente.$rowID, {
        ...novedad,
        lastModified: new Date().toISOString()
      });
      console.log(`Registro actualizado: ${registroExistente.$rowID}`);
      return { success: true, action: "update", id: registroExistente.$rowID };
    } else if (registroExistente) {
      console.error("Error: El registro existente no tiene un $rowID.");
      return { success: false, error: "Registro existente sin $rowID" };
    } else {
      const insertedId = await ticketsTable.add({
        ...novedad,
        fechaCreaciN: esFechaValida(novedad.fechaCreaciN)
          ? new Date(novedad.fechaCreaciN).toISOString()
          : new Date().toISOString(),
        fechaInicio: new Date(novedad.fechaInicio).toISOString(),
        fechaFin: new Date(novedad.fechaFin).toISOString(),
        lastModified: new Date().toISOString()
      });
      console.log(`Novedad insertada: ${insertedId}`);
      return { success: true, action: "add", id: insertedId };
    }
  } catch (error) {
    console.error("Error al sincronizar novedad:", error.message);
    return { success: false, error: error.message };
  }
}

if (process.argv[2] === "get") {
  ticketsTable
    .get()
    .then((registros) => {
      console.log("Registros obtenidos:", registros);
    })
    .catch((error) => {
      console.error("Error al obtener registros:", error.message);
    });
}

if (process.argv[2] === "test") {
  const ejemplo = {
    idCreador: "JUM-0001",
    novedad: "PRUEBA MANUAL",
    fechaCreaciN: "2025-05-06",
    idEmpleado: "JUM-9999",
    fechaInicio: "2025-05-06",
    fechaFin: "2025-05-08",
    comentario: "Desde script",
    attachment: "",
    validaciN: false,
    idValidador: "",
    regiNAsignadaDesdeEjecuciN: "",
    eliminar: false
  };
  syncNovedad(ejemplo).then((res) => console.log("Resultado:", res));
}

export async function deleteNovedad(rowID) {
  try {
    await ticketsTable.delete(rowID);
    console.log(`Registro eliminado: ${rowID}`);
    return { success: true, action: "delete", id: rowID };
  } catch (error) {
    console.error("Error al eliminar registro:", error.message);
    return { success: false, error: error.message };
  }
}

if (process.argv[2] && process.argv[2].startsWith("{")) {
  const novedad = JSON.parse(process.argv[2]);
  syncNovedad(novedad).then((res) => console.log("→", res));
}

if (process.argv[2] === "bulk") {
  try {
    const registros = JSON.parse(fs.readFileSync("a_enviar.json", "utf8"));
    console.log(`🧾 Procesando ${registros.length} novedades...`);

    for (const novedad of registros) {
      const resultado = await syncNovedad(novedad);
      console.log("→", resultado);
    }

    process.exit(0);
  } catch (error) {
    console.error("❌ Error en modo bulk:", error.message);
    process.exit(1);
  }
}