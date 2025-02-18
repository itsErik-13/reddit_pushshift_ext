import libtorrent as lt
import os
import time

def descargar_archivos_torrent(ruta_torrent, meses):
    try:
        
        lt_version = lt.version
        print(f"🔹 Usando libtorrent versión: {lt_version}")

        ses = lt.session()

        if lt_version.startswith("2."):
            settings = ses.get_settings()
            settings["listen_interfaces"] = '0.0.0.0:6881'  # Escuchar en todas las interfaces
            settings["connections_limit"] = 200  # Aumentar número de conexiones
            settings["active_downloads"] = 10  # Aumentar número de descargas simultáneas
            settings["active_seeds"] = 5  # Aumentar número de seeds activos
            settings["enable_dht"] = True  # Activar DHT para encontrar más peers
            settings["download_rate_limit"] = 0  # Sin límite de descarga
            settings["upload_rate_limit"] = 0  # Sin límite de subida
            ses.apply_settings(settings)
        else:
            settings = lt.settings_pack()
            settings.set_str(lt.settings_pack.listen_interfaces, '0.0.0.0:6881')
            settings.set_int(lt.settings_pack.connections_limit, 200)
            settings.set_int(lt.settings_pack.active_downloads, 10)
            settings.set_int(lt.settings_pack.active_seeds, 5)
            settings.set_bool(lt.settings_pack.enable_dht, True)
            settings.set_int(lt.settings_pack.download_rate_limit, 0)
            settings.set_int(lt.settings_pack.upload_rate_limit, 0)
            ses.apply_settings(settings)

        # Cargar torrent
        with open(ruta_torrent, 'rb') as f:
            e = lt.bdecode(f.read())

        info = lt.torrent_info(e)
        directorio_descarga = "downloads"
        os.makedirs(directorio_descarga, exist_ok=True)

        # Agregar torrent a la sesión
        params = {
            'ti': info,
            'save_path': directorio_descarga,
            'storage_mode': lt.storage_mode_t.storage_mode_sparse, 
        }
        h = ses.add_torrent(params)
        print(f"📂 Torrent cargado: {h.status().name}")

        # Seleccionar solo archivos específicos dentro del torrent, en este caso, los que contienen los meses especificados
        archivos = info.files()
        archivos_seleccionados = 0

        for i, archivo in enumerate(archivos):
            ruta_archivo = archivo.path
            nombre_archivo = os.path.basename(ruta_archivo)

            if "reddit/submissions" in ruta_archivo:
                mes_archivo = nombre_archivo.split("_")[1].split(".")[0]

                if mes_archivo in meses:
                    h.file_priority(i, 1)  # Descargar
                    archivos_seleccionados += 1
                    print(f" Descargando: {ruta_archivo}")
                else:
                    h.file_priority(i, 0)  #  No descargar
                    
            else:
                h.file_priority(i, 0)  #  No descargar

        if archivos_seleccionados == 0:
            print("No se encontraron archivos con los meses especificados. Saliendo.")
            return

        # Monitorear la descarga con actualización en tiempo real
        while not h.status().is_seeding:
            s = h.status()
            if s.download_rate > 0:
                tiempo_restante = (1 - s.progress) * (s.total_wanted - s.total_done) / s.download_rate
                tiempo_restante_horas = int(tiempo_restante / 3600)
                tiempo_restante_minutos = int((tiempo_restante % 3600) / 60)
                tiempo_restante_segundos = int(tiempo_restante % 60)
                tiempo_restante_str = f"{tiempo_restante_horas:02}:{tiempo_restante_minutos:02}:{tiempo_restante_segundos:02}"
            else:
                tiempo_restante_str = "Calculando..."
            print(f"📦 Progreso: {s.progress * 100:.2f}% \t| 🔽 {s.download_rate / 1000:.2f} kB/s \t| Peers: {s.num_peers} \t| ⏳ Tiempo Restante: {tiempo_restante_str}")
            time.sleep(5)

        print("¡Descarga completada!")

    except Exception as e:
        print(f"Error: {e}")

#Ejemplo
ruta_torrent = "reddit_db.torrent"
meses_a_descargar = ["2023-10"]
descargar_archivos_torrent(ruta_torrent, meses_a_descargar)
