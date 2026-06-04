import hashlib
import logging

logger = logging.getLogger(__name__)

ANIMALS = [
    "tigre",
    "leon",
    "perro",
    "gato",
    "raton",
    "elefante",
    "jirafa",
    "mono",
    "oso",
    "lobo",
    "zorro",
    "liebre",
    "tortuga",
    "buho",
    "aguila",
    "delfin",
    "tiburon",
    "ballena",
    "pulpo",
    "cangrejo",
    "cebra",
    "cocodrilo",
    "koala",
    "panda",
    "camello",
    "caballo",
    "oveja",
    "cabra",
    "gallina",
    "pato",
    "bisonte",
    "hipopotamo",
    "rinoceronte",
    "leopardo",
    "pantera",
    "guepardo",
    "ciervo",
    "alce",
    "ardilla",
    "castor",
]

ADJECTIVES = [
    "miedica",
    "valiente",
    "rapido",
    "lento",
    "grande",
    "pequeño",
    "alegre",
    "triste",
    "astuto",
    "torpe",
    "fuerte",
    "debil",
    "manso",
    "feroz",
    "tranquilo",
    "inquieto",
    "timido",
    "audaz",
    "fiel",
    "perezoso",
    "curioso",
    "travieso",
    "amigable",
    "solitario",
    "generoso",
    "tacaño",
    "paciente",
    "impaciente",
    "educado",
    "grosero",
    "limpio",
    "sucio",
    "ordenado",
    "desordenado",
    "inteligente",
    "despistado",
    "orgulloso",
    "humilde",
    "agradecido",
    "chistoso",
]


def generate_valid_combinations(seed: str, num_combinations: int = 300) -> set[str]:
    all_combos = [f"{a}_{adj}" for a in ANIMALS for adj in ADJECTIVES]
    # Hashing determinista para evitar dependencia en random.Random interno de Python
    all_combos.sort(key=lambda c: hashlib.sha256(f"{c}:{seed}".encode()).hexdigest())
    return set(all_combos[:num_combinations])


def normalize_combination(client_id: str) -> str:
    cleaned = client_id.strip().lower()
    cleaned = cleaned.replace(" ", "_").replace("-", "_")
    parts = [p.strip() for p in cleaned.split("_") if p.strip()]
    if len(parts) == 2:
        return f"{parts[0]}_{parts[1]}"
    return cleaned
