#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PiGoPerch (anciennement « BirdNET 3 colonnes »)
===============================================
Combine les détections de trois détecteurs dans une seule page web,
conçue pour être consultée sur un téléphone :
    BirdNET-Pi, BirdNET-Go (modèle BirdNET) et Perch

- BirdNET-Pi : lecture directe de sa base de données birds.db (ou par MQTT au choix).
- BirdNET-Go et Perch : reçus par MQTT, puis conservés dans une petite base de
  données propre à ce programme, pour pouvoir consulter les jours passés.

Une seule ligne par espèce, avec une cote de fiabilité selon l'accord entre
les détecteurs (Élevée, Moyenne, Faible).

Un calendrier permet de choisir la date à afficher.

Installation (une seule fois) :
    sudo apt install python3-paho-mqtt

Lancement :
    python3 PiGoPerch.py

Affichage :
    Dans un navigateur : http://ADRESSE-IP-DU-PI:8090
"""

import bisect
import json
import os
import re
import socket
import sqlite3
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, date, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

import paho.mqtt.client as mqtt

# =====================================================================
#  CONFIGURATION — à adapter
# =====================================================================

# Les valeurs personnelles (mot de passe, position...) se placent de préférence
# dans le fichier ~/pigoperch.json (voir plus bas), pas dans ce script.

MQTT_BROKER = "192.168.1.100"   # Adresse IP du broker MQTT
MQTT_PORT = 1883
MQTT_USER = ""                  # Nom d'utilisateur du broker ("" = aucun)
MQTT_PASS = ""                  # Mot de passe du broker

# --- BirdNET-Pi ---
# "base" : lit directement la base de données de BirdNET-Pi (le programme doit
#          tourner sur le même Pi que BirdNET-Pi). Recommandé.
# "mqtt" : reçoit les détections de BirdNET-Pi par MQTT (sujets ci-dessous).
SOURCE_BIRDNET_PI = "base"
BASE_BIRDNET_PI = os.path.expanduser("~/BirdNET-Pi/scripts/birds.db")

# Sujets (topics) MQTT. Le « # » veut dire « tout ce qui suit ».
TOPICS_BIRDNET_PI = ["birdpi/#"]    # Utilisé seulement si SOURCE_BIRDNET_PI = "mqtt"
TOPICS_BIRDNET_GO = ["birdnet/#"]

# --- Base de données de ce programme (historique BirdNET-Go et Perch) ---
BASE_LOCALE = os.path.expanduser("~/birdnet_3colonnes.db")
GARDER_JOURS = 730              # Nombre de jours d'historique conservés (2 ans)

PORT_WEB = 8090                 # Port de la page web
FENETRE_CONFIRMATION = 120      # Secondes : écart maximal pour considérer une confirmation
CONF_MIN = 0.0                  # Confiance minimale pour afficher (0.0 = tout afficher)

# --- eBird : badge « No50Km » ---
# Une espèce reçoit le badge No50Km si personne ne l'a signalée sur eBird à moins
# de EBIRD_RAYON_KM km dans les EBIRD_JOURS jours précédant la date affichée.
# La clé API (gratuite : https://ebird.org/api/keygen) est lue dans ce fichier,
# pour qu'elle n'apparaisse jamais dans le script.
EBIRD_FICHIER_CLE = os.path.expanduser("~/.ebird_api_key")
EBIRD_LAT = None                # Latitude de la station (ex. 45.50)
EBIRD_LNG = None                # Longitude de la station (ex. -73.57)
EBIRD_RAYON_KM = 50             # 50 km = maximum permis par l'API eBird
EBIRD_JOURS = 7
EBIRD_CACHE_HEURES = 6          # Fréquence de mise à jour d'une espèce
EBIRD_TAXONOMIE = os.path.expanduser("~/ebird_taxonomie.json")

# --- Réglages personnels, hors du script ---
# Toute variable en MAJUSCULES ci-dessus peut être remplacée dans ce fichier JSON,
# par exemple :
#   {"MQTT_BROKER": "192.168.1.20", "MQTT_USER": "moi", "MQTT_PASS": "secret",
#    "EBIRD_LAT": 45.50, "EBIRD_LNG": -73.57}
FICHIER_REGLAGES = os.path.expanduser("~/pigoperch.json")
try:
    with open(FICHIER_REGLAGES, encoding="utf-8") as _f:
        globals().update({k: v for k, v in json.load(_f).items() if k.isupper()})
except OSError:
    pass
except ValueError as _err:
    print("Fichier %s illisible (JSON invalide) : %s" % (FICHIER_REGLAGES, _err))

# =====================================================================
#  État général
# =====================================================================

verrou = threading.Lock()
etat = {"connecte": False, "dernier_message": None, "total": 0, "erreur": "",
        "pi": "", "pi_ok": False}

NOMS_COMMUN = ["commonname", "common_name", "comname", "com_name", "species", "espece"]
NOMS_SCIENT = ["scientificname", "scientific_name", "sciname", "sci_name"]
NOMS_CONF = ["confidence", "confidence_score", "conf", "score"]
NOMS_HEURE = ["time", "heure", "begintime", "detected_at", "timestamp", "ts", "datetime"]


# =====================================================================
#  Base de données locale (historique)
# =====================================================================

verrou_base = threading.Lock()
base = sqlite3.connect(BASE_LOCALE, check_same_thread=False, timeout=10)
base.execute("""CREATE TABLE IF NOT EXISTS detections (
                    col   TEXT NOT NULL,      -- 'pi', 'go' ou 'perch'
                    t     REAL NOT NULL,      -- moment de la détection (secondes)
                    jour  TEXT NOT NULL,      -- AAAA-MM-JJ
                    heure TEXT NOT NULL,      -- HH:MM:SS
                    nom   TEXT NOT NULL,
                    sci   TEXT,
                    conf  REAL)""")
base.execute("CREATE INDEX IF NOT EXISTS idx_jour ON detections (jour, col)")
# Espèces confirmées à la main (entendues ou vues avec certitude) : permanent
base.execute("""CREATE TABLE IF NOT EXISTS confirmations (
                    cle   TEXT PRIMARY KEY,   -- nom scientifique (minuscules) ou nom
                    nom   TEXT,
                    quand TEXT NOT NULL)      -- AAAA-MM-JJ HH:MM
                    """)
base.commit()


def lire_confirmations():
    """Retourne {cle: quand} pour toutes les espèces confirmées."""
    with verrou_base:
        return dict(base.execute("SELECT cle, quand FROM confirmations").fetchall())


def changer_confirmation(cle, nom, confirme):
    with verrou_base:
        if confirme:
            base.execute("INSERT OR REPLACE INTO confirmations (cle, nom, quand) VALUES (?, ?, ?)",
                         (cle, nom, datetime.now().strftime("%Y-%m-%d %H:%M")))
        else:
            base.execute("DELETE FROM confirmations WHERE cle = ?", (cle,))
        base.commit()


def enregistrer(col, e):
    jour = datetime.fromtimestamp(e["t"]).strftime("%Y-%m-%d")
    with verrou_base:
        base.execute("INSERT INTO detections (col, t, jour, heure, nom, sci, conf) "
                     "VALUES (?, ?, ?, ?, ?, ?, ?)",
                     (col, e["t"], jour, e["heure"], e["nom"], e["sci"], e["conf"]))
        base.commit()


def purger_vieux():
    limite = (date.today() - timedelta(days=GARDER_JOURS)).strftime("%Y-%m-%d")
    with verrou_base:
        base.execute("DELETE FROM detections WHERE jour < ?", (limite,))
        base.commit()


def lire_locale(jour):
    """Retourne {col: [(t, heure, nom, sci, conf), ...]} pour une journée."""
    res = {"pi": [], "go": [], "perch": []}
    with verrou_base:
        lignes = base.execute("SELECT col, t, heure, nom, sci, conf FROM detections "
                              "WHERE jour = ? ORDER BY t", (jour,)).fetchall()
    for col, t, h, nom, sci, conf in lignes:
        if col in res:
            res[col].append((t, h, nom, sci or "", conf))
    return res


def premier_jour_local():
    with verrou_base:
        r = base.execute("SELECT MIN(jour) FROM detections").fetchone()
    return r[0] if r and r[0] else None


# =====================================================================
#  Base de données de BirdNET-Pi (lecture seule)
# =====================================================================

def ouvrir_base_pi():
    # Ouverture en lecture seule : aucun risque d'abîmer la base de BirdNET-Pi
    return sqlite3.connect("file:%s?mode=ro" % BASE_BIRDNET_PI, uri=True, timeout=10)


def lire_birdnet_pi(jour):
    """Retourne [(t, heure, nom, sci, conf), ...] de BirdNET-Pi pour une journée."""
    try:
        if not os.path.exists(BASE_BIRDNET_PI):
            raise FileNotFoundError("base introuvable : %s" % BASE_BIRDNET_PI)
        con = ouvrir_base_pi()
        try:
            lignes = con.execute("SELECT Date, Time, Sci_Name, Com_Name, Confidence "
                                 "FROM detections WHERE Date = ? ORDER BY Time",
                                 (jour,)).fetchall()
        finally:
            con.close()
        sortie = []
        for d, h, sci, nom, conf in lignes:
            h = str(h or "")[:8]
            try:
                t = datetime.strptime("%s %s" % (d, h), "%Y-%m-%d %H:%M:%S").timestamp()
            except (ValueError, TypeError):
                continue
            try:
                conf = float(conf)
                if conf > 1.0:
                    conf = conf / 100.0
            except (ValueError, TypeError):
                conf = None
            sortie.append((t, h, str(nom or sci or "?"), str(sci or ""), conf))
        with verrou:
            etat["pi_ok"] = True
            etat["pi"] = "Base BirdNET-Pi lue à %s" % datetime.now().strftime("%H:%M:%S")
        return sortie
    except Exception as err:
        with verrou:
            etat["pi_ok"] = False
            etat["pi"] = "Erreur de lecture de la base BirdNET-Pi : %s" % err
        return []


def premier_jour_pi():
    try:
        con = ouvrir_base_pi()
        try:
            r = con.execute("SELECT MIN(Date) FROM detections").fetchone()
        finally:
            con.close()
        return r[0] if r and r[0] else None
    except Exception:
        return None


# =====================================================================
#  Lecture des messages MQTT
# =====================================================================

def trouver(d, noms):
    """Cherche une valeur par nom de clé (sans tenir compte des majuscules)."""
    if not isinstance(d, dict):
        return None
    minus = {str(k).lower(): v for k, v in d.items()}
    for n in noms:
        if n in minus and minus[n] not in (None, ""):
            return minus[n]
    for v in d.values():
        if isinstance(v, dict):
            r = trouver(v, noms)
            if r is not None:
                return r
    return None


def texte_heure(valeur, recu):
    """Transforme l'heure du message en HH:MM:SS, sinon utilise l'heure de réception."""
    if valeur is not None:
        if isinstance(valeur, (int, float)) or (isinstance(valeur, str) and re.fullmatch(r"\d+(\.\d+)?", valeur)):
            try:
                v = float(valeur)
                if v > 1e12:          # millisecondes
                    v = v / 1000.0
                if v > 1e9:
                    return datetime.fromtimestamp(v).strftime("%H:%M:%S")
            except (ValueError, OSError):
                pass
        if isinstance(valeur, str):
            m = re.search(r"(\d{1,2}:\d{2}:\d{2})", valeur)
            if m:
                return m.group(1).zfill(8)
    return datetime.fromtimestamp(recu).strftime("%H:%M:%S")


def ramasser_textes(obj, sortie):
    """Ramasse tous les textes (valeurs et clés) d'un objet, en minuscules."""
    if isinstance(obj, str):
        sortie.append(obj.lower())
    elif isinstance(obj, dict):
        for k, v in obj.items():
            sortie.append(str(k).lower())
            ramasser_textes(v, sortie)
    elif isinstance(obj, list):
        for x in obj:
            ramasser_textes(x, sortie)


def modeles_mentionnes(obj, sortie):
    """Cherche les noms de modèles dans les clés contenant « model » ou « classifier »."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            kl = str(k).lower()
            if "model" in kl or "classifier" in kl:
                ramasser_textes(v, sortie)
            modeles_mentionnes(v, sortie)
    elif isinstance(obj, list):
        for x in obj:
            modeles_mentionnes(x, sortie)


def colonnes_birdnet_go(donnees):
    """Décide si un message de BirdNET-Go vient du modèle BirdNET, de Perch, ou des deux."""
    textes = []
    modeles_mentionnes(donnees, textes)
    resultat = set()
    for t in textes:
        if "perch" in t:
            resultat.add("perch")
        if "birdnet" in t:
            resultat.add("go")
    if not resultat:
        resultat.add("go")    # Par défaut : modèle BirdNET
    return resultat


def sujets_abonnes():
    if SOURCE_BIRDNET_PI == "mqtt":
        return TOPICS_BIRDNET_PI + TOPICS_BIRDNET_GO
    return list(TOPICS_BIRDNET_GO)


def appareil_du_sujet(sujet):
    if SOURCE_BIRDNET_PI == "mqtt":
        for s in TOPICS_BIRDNET_PI:
            if mqtt.topic_matches_sub(s, sujet):
                return "pi"
    for s in TOPICS_BIRDNET_GO:
        if mqtt.topic_matches_sub(s, sujet):
            return "go"
    return None


def traiter_message(sujet, texte):
    recu = time.time()
    with verrou:
        etat["dernier_message"] = datetime.fromtimestamp(recu).strftime("%H:%M:%S")
        etat["total"] += 1

    appareil = appareil_du_sujet(sujet)
    if appareil is None:
        return

    try:
        donnees = json.loads(texte)
    except (ValueError, TypeError):
        donnees = None

    if isinstance(donnees, dict):
        nom = trouver(donnees, NOMS_COMMUN)
        sci = trouver(donnees, NOMS_SCIENT)
        conf = trouver(donnees, NOMS_CONF)
        heure = texte_heure(trouver(donnees, NOMS_HEURE), recu)
    else:
        nom, sci, conf = texte.strip(), None, None
        heure = texte_heure(None, recu)

    if not nom and not sci:
        return
    nom = str(nom) if nom else str(sci)
    sci = str(sci) if sci else ""

    try:
        conf = float(conf) if conf is not None else None
        if conf is not None and conf > 1.0:
            conf = conf / 100.0
    except (ValueError, TypeError):
        conf = None

    if conf is not None and conf < CONF_MIN:
        return

    # Moment de la détection : l'heure indiquée dans le message si elle est
    # vraisemblable (moins de 10 minutes d'écart), sinon l'heure de réception.
    t = recu
    try:
        h = datetime.strptime(heure, "%H:%M:%S").time()
        candidat = datetime.combine(datetime.fromtimestamp(recu).date(), h).timestamp()
        for essai in (candidat, candidat - 86400):     # message juste avant minuit
            if abs(recu - essai) <= 600:
                t = essai
                break
    except (ValueError, TypeError):
        pass
    if t == recu:
        # Heure du message invraisemblable : on affiche l'heure de réception
        heure = datetime.fromtimestamp(recu).strftime("%H:%M:%S")

    entree = {"t": t, "heure": heure, "nom": nom, "sci": sci, "conf": conf}

    if appareil == "pi":
        cibles = {"pi"}
    else:
        cibles = colonnes_birdnet_go(donnees) if isinstance(donnees, dict) else {"go"}

    for c in cibles:
        enregistrer(c, entree)


# =====================================================================
#  MQTT
# =====================================================================

RAISONS_REFUS = {
    1: "version du protocole refusée",
    2: "identifiant client refusé",
    3: "broker indisponible",
    4: "nom d'utilisateur ou mot de passe invalide",
    5: "non autorisé : le broker exige un nom d'utilisateur et un mot de passe valides",
    134: "nom d'utilisateur ou mot de passe invalide",
    135: "non autorisé : le broker exige un nom d'utilisateur et un mot de passe valides",
}


def sur_connexion(client, userdata, flags, rc, *args):
    try:
        code = int(getattr(rc, "value", rc))
    except (TypeError, ValueError):
        code = -1
    if code == 0:
        with verrou:
            etat["connecte"] = True
            etat["erreur"] = ""
        for s in sujets_abonnes():
            client.subscribe(s)
        print("Connecté au broker MQTT, abonné à :", sujets_abonnes())
    else:
        with verrou:
            etat["connecte"] = False
            etat["erreur"] = "Connexion refusée (code %s : %s)" % (code, RAISONS_REFUS.get(code, "raison inconnue"))
        print("Connexion MQTT refusée, code %s : %s" % (code, RAISONS_REFUS.get(code, "raison inconnue")))


def sur_deconnexion(client, userdata, *args):
    with verrou:
        etat["connecte"] = False
    print("Déconnecté du broker MQTT, nouvelle tentative automatique...")


def sur_message(client, userdata, msg):
    try:
        # Message « conservé » (retained) par le broker : c'est une vieille détection
        # renvoyée à chaque reconnexion, pas un nouveau chant. On l'ignore.
        if getattr(msg, "retain", False):
            return
        texte = msg.payload.decode("utf-8", errors="replace")
        traiter_message(msg.topic, texte)
    except Exception as e:
        print("Erreur de traitement :", e)


def demarrer_mqtt():
    try:
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="birdnet-3colonnes")
    except AttributeError:
        client = mqtt.Client(client_id="birdnet-3colonnes")
    if MQTT_USER:
        client.username_pw_set(MQTT_USER, MQTT_PASS)
    client.on_connect = sur_connexion
    client.on_disconnect = sur_deconnexion
    client.on_message = sur_message
    client.reconnect_delay_set(min_delay=2, max_delay=60)
    client.connect_async(MQTT_BROKER, MQTT_PORT, keepalive=60)
    client.loop_start()
    return client


# =====================================================================
#  Préparation des données d'une journée
# =====================================================================

# Noms scientifiques différents pour une même espèce (anciens et nouveaux noms).
# BirdNET-Pi utilise parfois l'ancien nom, BirdNET-Go le nouveau.
# Format : "ancien nom" : "nom utilisé", en minuscules.
SYNONYMES = {
    "coccothraustes vespertinus": "hesperiphona vespertina",    # Gros-bec errant
    "carduelis tristis": "spinus tristis",                      # Chardonneret jaune
    "carduelis pinus": "spinus pinus",                          # Tarin des pins
    "carpodacus mexicanus": "haemorhous mexicanus",             # Roselin familier
    "carpodacus purpureus": "haemorhous purpureus",             # Roselin pourpré
    "dendroica coronata": "setophaga coronata",                 # Paruline à croupion jaune
    "dendroica petechia": "setophaga petechia",                 # Paruline jaune
    "wilsonia pusilla": "cardellina pusilla",                   # Paruline à calotte noire
    "picoides pubescens": "dryobates pubescens",                # Pic mineur
    "picoides villosus": "dryobates villosus",                  # Pic chevelu
    "oporornis philadelphia": "geothlypis philadelphia",        # Paruline triste
    "vermivora peregrina": "leiothlypis peregrina",             # Paruline obscure
    "vermivora ruficapilla": "leiothlypis ruficapilla",         # Paruline à joues grises
    "seiurus noveboracensis": "parkesia noveboracensis",        # Paruline des ruisseaux
    "anas americana": "mareca americana",                       # Canard d'Amérique
    "anas strepera": "mareca strepera",                         # Canard chipeau
    "anas clypeata": "spatula clypeata",                        # Canard souchet
    "anas discors": "spatula discors",                          # Sarcelle à ailes bleues
}


def cle_espece(sci, nom):
    """Clé qui identifie une espèce, en tenant compte des synonymes."""
    s = (sci or "").strip().lower()
    if s:
        return SYNONYMES.get(s, s)
    return (nom or "").strip().lower()


# =====================================================================
#  eBird : l'espèce a-t-elle été signalée à moins de 50 km récemment ?
# =====================================================================

verrou_ebird = threading.Lock()
ebird = {"codes": None, "cache": {}, "attente": [], "erreur": "", "maj": None}
EBIRD_RECUL_MAX = 30            # L'API eBird ne remonte pas plus loin que 30 jours


def ebird_cle():
    try:
        with open(EBIRD_FICHIER_CLE, encoding="utf-8") as f:
            return f.read().strip()
    except OSError:
        return ""


def ebird_get(chemin, cle):
    req = urllib.request.Request("https://api.ebird.org/v2/" + chemin,
                                 headers={"X-eBirdApiToken": cle})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode("utf-8"))


def ebird_codes(cle):
    """Table nom scientifique (minuscules) -> code d'espèce eBird (ex. barswa)."""
    if ebird["codes"] is not None:
        return ebird["codes"]
    try:
        if time.time() - os.path.getmtime(EBIRD_TAXONOMIE) < 30 * 86400:
            with open(EBIRD_TAXONOMIE, encoding="utf-8") as f:
                ebird["codes"] = json.load(f)
                return ebird["codes"]
    except (OSError, ValueError):
        pass
    taxo = ebird_get("ref/taxonomy/ebird?fmt=json&cat=species", cle)
    codes = {t["sciName"].lower(): t["speciesCode"]
             for t in taxo if t.get("sciName") and t.get("speciesCode")}
    tmp = EBIRD_TAXONOMIE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(codes, f)
    os.replace(tmp, EBIRD_TAXONOMIE)
    ebird["codes"] = codes
    return codes


def ebird_dates(sci):
    """Dates (AAAA-MM-JJ) des observations eBird récentes à moins de 50 km.
    Retourne None si inconnu (pas encore vérifié, pas une espèce eBird, erreur).
    Une vérification est demandée en arrière-plan si nécessaire."""
    with verrou_ebird:
        c = ebird["cache"].get(sci)
        if (c is None or time.time() - c[0] > EBIRD_CACHE_HEURES * 3600) \
                and sci not in ebird["attente"]:
            ebird["attente"].append(sci)
        return c[1] if c else None


def travail_ebird():
    """Fil d'arrière-plan : interroge eBird une espèce à la fois, sans se presser."""
    while True:
        with verrou_ebird:
            sci = ebird["attente"].pop(0) if ebird["attente"] else None
        if sci is None:
            time.sleep(2)
            continue
        cle = ebird_cle()
        if EBIRD_LAT is None or EBIRD_LNG is None:
            with verrou_ebird:
                ebird["erreur"] = "position non configurée (EBIRD_LAT / EBIRD_LNG)"
                ebird["attente"].clear()
            time.sleep(60)
            continue
        if not cle:
            with verrou_ebird:
                ebird["erreur"] = "clé API absente (%s)" % EBIRD_FICHIER_CLE
                ebird["attente"].clear()
            time.sleep(60)
            continue
        try:
            code = ebird_codes(cle).get(sci)
            if code is None:
                dates = None        # Pas une espèce eBird (ex. : bruit, humain, chien)
            else:
                obs = ebird_get("data/obs/geo/recent/%s?lat=%s&lng=%s&dist=%d&back=%d"
                                "&includeProvisional=true"
                                % (code, EBIRD_LAT, EBIRD_LNG, EBIRD_RAYON_KM, EBIRD_RECUL_MAX), cle)
                dates = sorted({str(o.get("obsDt", ""))[:10] for o in obs if o.get("obsDt")})
            with verrou_ebird:
                ebird["cache"][sci] = (time.time(), dates)
                ebird["erreur"] = ""
                ebird["maj"] = datetime.now().strftime("%H:%M:%S")
        except Exception as err:
            if isinstance(err, urllib.error.HTTPError) and err.code in (401, 403):
                msg = "clé API refusée par eBird"
            else:
                msg = str(err)
            with verrou_ebird:
                ebird["erreur"] = msg
                ancien = ebird["cache"].get(sci)
                # Nouvel essai dans environ 10 minutes
                ebird["cache"][sci] = (time.time() - EBIRD_CACHE_HEURES * 3600 + 600,
                                       ancien[1] if ancien else None)
        time.sleep(1)


def no50km(sci, jour):
    """Vrai si aucune observation eBird à moins de 50 km dans les 7 jours avant « jour »."""
    if not sci:
        return False
    auj = date.today()
    if jour > auj or jour < auj - timedelta(days=EBIRD_RECUL_MAX - EBIRD_JOURS):
        return False              # Hors de la période que l'API eBird peut couvrir
    dates = ebird_dates(cle_espece(sci, ""))
    if dates is None:
        return False
    debut = (jour - timedelta(days=EBIRD_JOURS)).strftime("%Y-%m-%d")
    # L'API ne donne que la plus récente observation de chaque lieu : une
    # observation postérieure à « jour » compte donc aussi comme présence.
    return not any(d >= debut for d in dates)


def etat_ebird():
    with verrou_ebird:
        if ebird["erreur"]:
            return False, "eBird : " + ebird["erreur"]
        if ebird["maj"]:
            return True, "eBird : %d espèce(s) vérifiée(s), dernière à %s" % (
                len(ebird["cache"]), ebird["maj"])
        return True, "eBird : en attente"


def regrouper(liste):
    """Regroupe les détections d'une colonne : une fiche par espèce."""
    fiches = {}
    for t, h, nom, sci, conf in liste:
        if conf is not None and conf < CONF_MIN:
            continue
        cle = cle_espece(sci, nom)
        f = fiches.get(cle)
        if f is None:
            f = {"nom": nom, "sci": sci, "premiere": t, "derniere": t,
                 "heure": h, "conf_max": conf, "nb": 0, "instants": []}
            fiches[cle] = f
        f["nb"] += 1
        f["instants"].append(t)
        if t >= f["derniere"]:
            f["derniere"] = t
            f["heure"] = h
        f["premiere"] = min(f["premiere"], t)
        if conf is not None and (f["conf_max"] is None or conf > f["conf_max"]):
            f["conf_max"] = conf
    for f in fiches.values():
        f["instants"].sort()
    return fiches


def confirme_par(instants_a, instants_b):
    """Vrai si au moins une détection de A est proche d'une détection de B."""
    for t in instants_a:
        i = bisect.bisect_left(instants_b, t - FENETRE_CONFIRMATION)
        if i < len(instants_b) and instants_b[i] <= t + FENETRE_CONFIRMATION:
            return True
    return False


def date_valide(texte):
    try:
        return datetime.strptime(texte, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return date.today()


def faire_synthese(toutes):
    """Combine les trois détecteurs en un seul tableau : une ligne par espèce.

    Fiabilité :
      3 = Élevée  : au moins deux détecteurs l'ont entendue au même moment (± fenêtre)
      2 = Moyenne : au moins deux détecteurs l'ont entendue dans la journée, mais pas au même moment
      1 = Faible  : un seul détecteur l'a entendue
    """
    ordre = ["pi", "go", "perch"]
    groupes = []          # liste de {col: fiche}
    par_cle = {}
    par_nom = {}
    for col in ordre:
        for cle, f in toutes[col].items():
            nom = f["nom"].strip().lower()
            g = par_cle.get(cle)
            if g is None:
                g = par_nom.get(nom)
            if g is None or col in g:
                g = {}
                groupes.append(g)
            g[col] = f
            par_cle.setdefault(cle, g)
            par_nom.setdefault(nom, g)

    lignes = []
    for g in groupes:
        cols = [c for c in ordre if c in g]
        # Nom affiché : on préfère un nom français (Pi d'abord, puis Go, puis Perch)
        nom = g[cols[0]]["nom"]

        simultanes = set()
        for i, a in enumerate(cols):
            for b in cols[i + 1:]:
                if confirme_par(g[a]["instants"], g[b]["instants"]):
                    simultanes.update((a, b))

        if simultanes:
            fiabilite = 3
        elif len(cols) >= 2:
            fiabilite = 2
        else:
            fiabilite = 1

        confs = [g[c]["conf_max"] for c in cols if g[c]["conf_max"] is not None]
        conf_moy = sum(confs) / len(confs) if confs else None

        premiere = min(g[c]["premiere"] for c in cols)
        derniere = max(g[c]["derniere"] for c in cols)
        lignes.append({
            "nom": nom,
            "sci": next((g[c]["sci"] for c in cols if g[c]["sci"]), ""),
            "cle": cle_espece(next((g[c]["sci"] for c in cols if g[c]["sci"]), ""), nom),
            "detecteurs": cols,
            "simultanes": sorted(simultanes, key=ordre.index),
            "fiabilite": fiabilite,
            "conf_moy": conf_moy,
            "nb": {c: g[c]["nb"] for c in cols},
            "premiere": datetime.fromtimestamp(premiere).strftime("%H:%M"),
            "derniere": datetime.fromtimestamp(derniere).strftime("%H:%M"),
            "_tri": (fiabilite, len(cols), conf_moy or 0),
        })

    lignes.sort(key=lambda x: x["_tri"], reverse=True)
    for x in lignes:
        del x["_tri"]
    return lignes


def donnees_page(jour_txt):
    jour = date_valide(jour_txt)
    jour_txt = jour.strftime("%Y-%m-%d")

    brutes = lire_locale(jour_txt)
    if SOURCE_BIRDNET_PI == "base":
        brutes["pi"] = lire_birdnet_pi(jour_txt)

    toutes = {col: regrouper(liste) for col, liste in brutes.items()}
    synthese = faire_synthese(toutes)
    confirmees = lire_confirmations()
    for x in synthese:
        x["no50"] = no50km(x["sci"], jour)
        x["confirme"] = confirmees.get(x["cle"])
    # Les espèces confirmées en tête (tri stable : l'ordre de fiabilité est gardé)
    synthese.sort(key=lambda x: not x["confirme"])

    debuts = [d for d in (premier_jour_local(),
                          premier_jour_pi() if SOURCE_BIRDNET_PI == "base" else None) if d]
    with verrou:
        e = dict(etat)
    e["ebird_ok"], e["ebird"] = etat_ebird()
    return {"synthese": synthese, "etat": e, "jour": jour_txt,
            "aujourdhui": date.today().strftime("%Y-%m-%d"),
            "premier_jour": min(debuts) if debuts else jour_txt,
            "debut_go": premier_jour_local(),
            "fenetre": FENETRE_CONFIRMATION}


# =====================================================================
#  Page web
# =====================================================================

PAGE_HTML = """<!doctype html>
<html lang="fr">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="theme-color" content="#0f1720">
<title>Oiseaux entendus (Par Roméo St-Cyr)</title>
<style>
  :root { --fond:#0f1720; --carte:#17212c; --bord:#2a3846; --texte:#e6edf3;
          --pale:#8b9bab; --pi:#e0a03a; --go:#4f9cf0; --perch:#5cc48a; --rouge:#f07070; }
  * { box-sizing: border-box; }
  html { -webkit-text-size-adjust: 100%; }
  body { margin:0; padding:12px 12px 24px; background:var(--fond); color:var(--texte);
         font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; }
  .page { max-width:640px; margin:0 auto; }
  h1 { font-size:1.25rem; margin:2px 0 10px; }

  /* Choix de la date */
  .barre { display:flex; gap:6px; align-items:stretch; }
  .barre button, .barre input { background:var(--carte); color:var(--texte);
         border:1px solid var(--bord); border-radius:10px; font-size:1rem; font-family:inherit;
         min-height:44px; }
  .barre button { padding:0 14px; cursor:pointer; }
  .barre button:disabled { opacity:.35; cursor:default; }
  .barre input { flex:1; min-width:0; padding:0 10px; color-scheme:dark; text-align:center; }
  #auj { width:100%; margin-top:6px; min-height:40px; }
  .jour { font-size:1.05rem; font-weight:600; margin:12px 2px 2px; }
  .direct { color:var(--perch); font-size:.82rem; font-weight:normal; }
  .resume { color:var(--pale); font-size:.88rem; margin:2px 2px 10px; }
  .avis { color:var(--pi); font-size:.85rem; margin:0 2px 10px; }

  /* Une carte par espèce */
  .esp { background:var(--carte); border:1px solid var(--bord); border-left-width:4px;
         border-radius:10px; padding:9px 11px; margin-bottom:7px; }
  .esp.f3 { border-left-color:var(--perch); }
  .esp.f2 { border-left-color:var(--pi); }
  .esp.f1 { border-left-color:var(--rouge); }
  .l1 { display:flex; justify-content:space-between; align-items:center; gap:8px; }
  .nom { font-size:1.05rem; font-weight:600; }
  .fiab { font-size:.75rem; padding:2px 8px; border-radius:8px; font-weight:600; white-space:nowrap; }
  .fiab.f3 { background:rgba(92,196,138,.2); color:var(--perch); }
  .fiab.f2 { background:rgba(224,160,58,.18); color:var(--pi); }
  .fiab.f1 { background:rgba(240,112,112,.15); color:var(--rouge); }
  .esp.f4 { border-left-color:#2ecc71; }
  .fiab.f4 { background:#1f8a4c; color:#fff; }
  .conf { margin-left:auto; background:transparent; color:var(--pale); border:1px solid var(--bord);
          border-radius:8px; font-size:.8rem; font-family:inherit; padding:3px 9px; min-height:30px; cursor:pointer; }
  .conf.on { background:rgba(46,204,113,.18); color:#2ecc71; border-color:#2ecc71; font-weight:600; }
  .droite { display:flex; gap:5px; align-items:center; }
  .no50 { font-size:.72rem; padding:1px 7px; border-radius:8px; font-weight:700; white-space:nowrap;
          color:#ff9f43; border:1px solid #ff9f43; background:rgba(255,159,67,.12); }
  .l2 { display:flex; flex-wrap:wrap; align-items:center; gap:5px 10px; margin-top:7px;
        font-size:.85rem; color:var(--pale); }
  .dets { display:flex; gap:4px; }
  .p { font-size:.75rem; padding:2px 7px; border-radius:8px; font-weight:600;
       color:#0f1720; white-space:nowrap; }
  .p-pi { background:var(--pi); } .p-go { background:var(--go); } .p-perch { background:var(--perch); }
  .p.absent { background:transparent; color:#4d5d6d; border:1px dashed #3a4a5a; font-weight:normal; }
  .chiffre { font-family:ui-monospace, Menlo, Consolas, monospace; color:var(--texte); }

  .vide { color:var(--pale); font-style:italic; padding:14px 2px; }
  .legende { color:var(--pale); font-size:.8rem; margin:16px 2px 6px; line-height:1.5; }
  .etat { color:var(--pale); font-size:.75rem; margin:8px 2px 0; line-height:1.5; }
  .ok { color:var(--perch); } .ko { color:var(--rouge); }
</style>
</head>
<body>
<div class="page">
  <h1>Oiseaux entendus (Roméo St-Cyr)</h1>

  <div class="barre">
    <button id="veille" aria-label="Jour précédent">◀</button>
    <input type="date" id="choix" aria-label="Choisir la date">
    <button id="lendemain" aria-label="Jour suivant">▶</button>
  </div>
  <div class="barre"><button id="auj">Aujourd'hui</button></div>

  <div class="jour" id="titre"></div>
  <div class="resume" id="resume"></div>
  <div class="avis" id="avis"></div>

  <div id="liste"></div>

  <div class="legende">
    <b style="color:#2ecc71">Confirmée</b> : espèce confirmée à la main (✓ Confirmé), pour tous les jours.
    Toucher de nouveau le bouton pour retirer la confirmation.<br>
    <b style="color:var(--perch)">Élevée</b> : au moins deux détecteurs au même moment (✓).<br>
    <b style="color:var(--pi)">Moyenne</b> : plusieurs détecteurs dans la journée, pas au même moment.<br>
    <b style="color:var(--rouge)">Faible</b> : un seul détecteur, à vérifier.<br>
    <span class="no50">No50Km</span> : aucune observation signalée sur eBird à moins de 50 km
    dans les 7 jours précédents (vérifiable pour les 23 derniers jours seulement).<br>
    Chiffre dans la pastille = nombre de détections. Pourcentage = confiance moyenne.
  </div>
  <div class="etat" id="etat">Chargement...</div>
</div>

<script>
const NOMS = { pi:"Pi", go:"Go", perch:"Perch" };
const JOURS = ["dimanche","lundi","mardi","mercredi","jeudi","vendredi","samedi"];
const MOIS = ["janvier","février","mars","avril","mai","juin","juillet","août",
              "septembre","octobre","novembre","décembre"];
const FIAB = { 4:"Confirmée", 3:"Élevée", 2:"Moyenne", 1:"Faible" };
let jourChoisi = "";      // "" = aujourd'hui
let aujourdhui = "";

function esc(s) { return String(s ?? "").replace(/[&<>"']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;","\\"":"&quot;","'":"&#39;"}[c])); }

function decaler(jourTxt, n) {
  const [a, m, j] = jourTxt.split("-").map(Number);
  const d = new Date(a, m - 1, j + n);
  return d.getFullYear() + "-" + String(d.getMonth() + 1).padStart(2, "0") + "-" + String(d.getDate()).padStart(2, "0");
}

function joli(jourTxt) {
  const [a, m, j] = jourTxt.split("-").map(Number);
  const d = new Date(a, m - 1, j);
  const t = JOURS[d.getDay()] + " " + (j === 1 ? "1er" : j) + " " + MOIS[m - 1] + " " + a;
  return t.charAt(0).toUpperCase() + t.slice(1);
}

function choisir(jourTxt) {
  jourChoisi = (jourTxt && jourTxt !== aujourdhui) ? jourTxt : "";
  try { history.replaceState(null, "", jourChoisi ? "#" + jourChoisi : location.pathname); } catch (e) {}
  rafraichir();
}

document.getElementById("choix").addEventListener("change", ev => choisir(ev.target.value));
document.getElementById("veille").addEventListener("click", () => choisir(decaler(jourChoisi || aujourdhui, -1)));
document.getElementById("lendemain").addEventListener("click", () => choisir(decaler(jourChoisi || aujourdhui, 1)));
document.getElementById("auj").addEventListener("click", () => choisir(""));

// Bouton « ✓ Confirmé » : bascule la confirmation permanente de l'espèce
document.getElementById("liste").addEventListener("click", async ev => {
  const b = ev.target.closest("button.conf");
  if (!b) return;
  b.disabled = true;
  try {
    await fetch("/confirmer", { method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ cle: b.dataset.cle, nom: b.dataset.nom, confirme: !b.classList.contains("on") }) });
  } catch (e) {}
  rafraichir();
});

async function rafraichir() {
  try {
    const r = await fetch("/api" + (jourChoisi ? "?date=" + jourChoisi : ""), { cache: "no-store" });
    const d = await r.json();
    aujourdhui = d.aujourdhui;
    const e = d.etat;
    const s = d.synthese;
    const estAuj = (d.jour === d.aujourdhui);

    const choix = document.getElementById("choix");
    choix.max = d.aujourdhui;
    choix.min = d.premier_jour;
    choix.value = d.jour;
    document.getElementById("lendemain").disabled = estAuj;
    document.getElementById("veille").disabled = (d.jour <= d.premier_jour);
    document.getElementById("auj").disabled = estAuj;
    document.getElementById("titre").innerHTML =
      esc(joli(d.jour)) + (estAuj ? ' <span class="direct">● en direct</span>' : "");

    const n = f => s.filter(x => x.fiabilite === f && !x.confirme).length;
    const nc = s.filter(x => x.confirme).length;
    document.getElementById("resume").textContent = s.length
      ? s.length + " espèce" + (s.length > 1 ? "s" : "") + " — " +
        (nc ? nc + " confirmée(s), " : "") + n(3) + " élevée(s), " + n(2) + " moyenne(s), " + n(1) + " faible(s)"
      : "";

    document.getElementById("avis").textContent =
      (!d.debut_go || d.jour < d.debut_go)
        ? "Pour cette date, seul BirdNET-Pi a un historique." : "";

    document.getElementById("liste").innerHTML = !s.length
      ? '<div class="vide">Aucune détection pour cette date</div>'
      : s.map(x => {
          const dets = ["pi", "go", "perch"].map(c =>
            x.detecteurs.includes(c)
              ? '<span class="p p-' + c + '">' + (x.simultanes.includes(c) ? "✓ " : "") + NOMS[c] + " " + x.nb[c] + '</span>'
              : '<span class="p absent">' + NOMS[c] + '</span>').join("");
          const conf = (x.conf_moy === null || x.conf_moy === undefined) ? "–" : Math.round(x.conf_moy * 100) + " %";
          const heures = x.premiere === x.derniere ? x.premiere : x.premiere + "–" + x.derniere;
          const f = x.confirme ? 4 : x.fiabilite;
          const bouton = '<button class="conf' + (x.confirme ? ' on' : '') + '" data-cle="' + esc(x.cle) +
                         '" data-nom="' + esc(x.nom) + '"' +
                         (x.confirme ? ' title="Confirmée le ' + esc(x.confirme) + ' — toucher pour retirer"' : '') +
                         '>' + (x.confirme ? '✓ Confirmé' : 'Confirmer') + '</button>';
          return '<div class="esp f' + f + '">' +
                   '<div class="l1"><span class="nom">' + esc(x.nom) + '</span>' +
                   '<span class="droite">' +
                   (x.no50 ? '<span class="no50" title="Aucune observation eBird à moins de 50 km dans les 7 jours précédents">No50Km</span>' : '') +
                   '<span class="fiab f' + f + '">' + FIAB[f] + '</span></span></div>' +
                   '<div class="l2"><span class="dets">' + dets + '</span>' +
                   '<span class="chiffre">' + conf + '</span>' +
                   '<span class="chiffre">' + esc(heures) + '</span>' + bouton + '</div>' +
                 '</div>';
        }).join("");

    document.getElementById("etat").innerHTML =
      (e.connecte ? '<span class="ok">●</span> MQTT connecté' : '<span class="ko">●</span> MQTT non connecté ' + esc(e.erreur)) +
      (e.dernier_message ? " — dernier message " + esc(e.dernier_message) : "") +
      (e.pi ? '<br>' + (e.pi_ok ? '<span class="ok">●</span> ' : '<span class="ko">●</span> ') + esc(e.pi) : "") +
      (e.ebird ? '<br>' + (e.ebird_ok ? '<span class="ok">●</span> ' : '<span class="ko">●</span> ') + esc(e.ebird) : "");
  } catch (err) {
    document.getElementById("etat").innerHTML = '<span class="ko">●</span> Programme injoignable';
  }
}

// Date demandée dans l'adresse (ex. : http://...:8090/#2026-11-03)
if (/^#\\d{4}-\\d{2}-\\d{2}$/.test(location.hash)) jourChoisi = location.hash.slice(1);
rafraichir();
// Mise à jour automatique seulement quand on regarde la journée en cours
setInterval(() => { if (!jourChoisi) rafraichir(); }, 5000);
// Au retour sur l'onglet (téléphone sorti de veille), mise à jour immédiate
document.addEventListener("visibilitychange", () => { if (!document.hidden && !jourChoisi) rafraichir(); });
</script>
</body>
</html>
"""


class Gestionnaire(BaseHTTPRequestHandler):
    def do_GET(self):
        adresse = urlparse(self.path)
        if adresse.path == "/api":
            jour = parse_qs(adresse.query).get("date", [""])[0]
            corps = json.dumps(donnees_page(jour), ensure_ascii=False).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
        elif adresse.path == "/":
            corps = PAGE_HTML.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
        else:
            corps = b"Introuvable"
            self.send_response(404)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(corps)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(corps)

    def do_POST(self):
        """POST /confirmer  {"cle": ..., "nom": ..., "confirme": true/false}"""
        code, corps = 200, b'{"ok": true}'
        if urlparse(self.path).path != "/confirmer":
            code, corps = 404, b'{"ok": false}'
        else:
            try:
                n = int(self.headers.get("Content-Length", 0))
                d = json.loads(self.rfile.read(min(n, 4096)).decode("utf-8"))
                cle = str(d.get("cle", "")).strip().lower()[:200]
                if not cle:
                    raise ValueError("clé vide")
                changer_confirmation(cle, str(d.get("nom", ""))[:200], bool(d.get("confirme")))
            except Exception:
                code, corps = 400, b'{"ok": false}'
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(corps)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(corps)

    def log_message(self, format, *args):
        pass   # Pas de journal pour chaque rafraîchissement de page


def entretien():
    """Fil d'arrière-plan : efface l'historique trop vieux, une fois par jour."""
    while True:
        try:
            purger_vieux()
        except Exception as err:
            print("Entretien de la base :", err)
        time.sleep(24 * 3600)


def adresse_ip_locale():
    """Trouve l'adresse IP de ce Pi sur le réseau local (aucune donnée n'est envoyée)."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect((MQTT_BROKER, MQTT_PORT))
        return s.getsockname()[0]
    except OSError:
        return "<adresse-ip-de-ce-pi>"
    finally:
        s.close()


def main():
    print("BirdNET 3 colonnes — démarrage")
    print("Broker MQTT :", MQTT_BROKER, "port", MQTT_PORT,
          "(utilisateur : %s)" % (MQTT_USER if MQTT_USER else "aucun"))
    print("Historique BirdNET-Go et Perch :", BASE_LOCALE)
    if SOURCE_BIRDNET_PI == "base":
        print("BirdNET-Pi : lecture directe de", BASE_BIRDNET_PI)
    else:
        print("BirdNET-Pi : réception par MQTT", TOPICS_BIRDNET_PI)
    threading.Thread(target=entretien, daemon=True).start()
    threading.Thread(target=travail_ebird, daemon=True).start()
    client = demarrer_mqtt()
    serveur = ThreadingHTTPServer(("0.0.0.0", PORT_WEB), Gestionnaire)
    print("Page web : http://%s:%d" % (adresse_ip_locale(), PORT_WEB))
    print("Ctrl+C pour arrêter.")
    try:
        serveur.serve_forever()
    except KeyboardInterrupt:
        print("\nArrêt demandé.")
    finally:
        try:
            client.disconnect()
        except Exception:
            pass
        serveur.server_close()
        try:
            with verrou_base:
                base.close()
        except Exception:
            pass
        print("Programme arrêté.")
        # Le fil MQTT est un fil d'arrière-plan : il s'arrête avec le programme.
        os._exit(0)


if __name__ == "__main__":
    main()
