# PiGoPerch

Une page web, pensée pour le téléphone, qui réunit en une seule liste les oiseaux
entendus par trois détecteurs :

- **BirdNET-Pi** (lecture directe de sa base `birds.db`)
- **BirdNET-Go**, modèle BirdNET (reçu par MQTT)
- **BirdNET-Go**, modèle **Perch v2** (reçu par MQTT)

Une ligne par espèce, avec une cote de fiabilité selon l'accord entre les détecteurs.

## Fonctions

- **Cote de fiabilité** : *Élevée* (au moins deux détecteurs au même moment, ± 2 min),
  *Moyenne* (plusieurs détecteurs dans la journée), *Faible* (un seul détecteur).
- **Calendrier** pour consulter les jours passés (historique de 2 ans).
- **Badge No50Km** : aucune observation de l'espèce signalée sur
  [eBird](https://ebird.org) à moins de 50 km dans les 7 jours précédents.
  Utile pour repérer une détection improbable (ex. une hirondelle en novembre).
  Calculable pour les 23 derniers jours (l'API eBird remonte 30 jours au plus).
- **Bouton Confirmer** : marque une espèce comme confirmée (entendue ou vue avec
  certitude), de façon permanente ; un second toucher retire la confirmation.

## Installation (Raspberry Pi, sur le même Pi que BirdNET-Pi)

```bash
sudo apt install python3-paho-mqtt
python3 PiGoPerch.py
```

Puis ouvrir `http://ADRESSE-IP-DU-PI:8090`.

### Réglages personnels

Les valeurs personnelles ne vont pas dans le script mais dans `~/pigoperch.json` :

```json
{
  "MQTT_BROKER": "192.168.1.20",
  "MQTT_USER": "utilisateur",
  "MQTT_PASS": "mot-de-passe",
  "EBIRD_LAT": 45.50,
  "EBIRD_LNG": -73.57
}
```

Toute variable en MAJUSCULES de la section CONFIGURATION du script peut y être remplacée.

### Clé eBird (pour le badge No50Km)

Obtenir une clé gratuite sur <https://ebird.org/api/keygen>, puis :

```bash
echo VOTRE_CLE > ~/.ebird_api_key && chmod 600 ~/.ebird_api_key
```

Sans clé ou sans position, la page fonctionne normalement, sans le badge.

### Service systemd (démarrage automatique)

```ini
[Unit]
Description=PiGoPerch (page web de comparaison)
After=network-online.target
Wants=network-online.target

[Service]
User=birdnet
WorkingDirectory=/home/birdnet
ExecStart=/usr/bin/python3 /home/birdnet/PiGoPerch.py
Restart=always
RestartSec=10
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
```

## Licence

MIT — voir [LICENSE](LICENSE).
