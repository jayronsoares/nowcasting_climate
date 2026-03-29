# stations.py
# 164 estações Operante — SP, RJ, MG, RS
# Fonte: https://apitempo.inmet.gov.br/estacoes/T (consultado Mar/2026)

STATIONS = {
    "SP": [
        ("A736", -21.133, -48.840),  # ARIRANHA
        ("A725", -23.101, -48.941),  # AVARE
        ("A746", -24.962, -48.416),  # BARRA DO TURVO
        ("A748", -20.559, -48.544),  # BARRETOS
        ("A755", -23.523, -46.869),  # BARUERI
        ("A705", -22.358, -49.028),  # BAURU
        ("A764", -20.948, -48.471),  # BEBEDOURO
        ("A765", -23.844, -46.143),  # BERTIOGA
        ("A744", -22.949, -46.526),  # BRAGANCA PAULISTA
        ("A769", -22.688, -45.005),  # CACHOEIRA PAULISTA
        ("A738", -21.780, -47.075),  # CASA BRANCA
        ("A708", -20.584, -47.382),  # FRANCA
        ("A737", -21.855, -48.799),  # IBITINGA
        ("A712", -24.671, -47.545),  # IGUAPE
        ("A713", -23.426, -47.585),  # IPERO
        ("A714", -23.981, -48.885),  # ITAPEVA
        ("A739", -22.415, -46.805),  # ITAPIRA
        ("A753", -20.359, -47.775),  # ITUVERAVA
        ("A733", -20.165, -50.594),  # JALES
        ("A735", -21.085, -49.920),  # JOSE BONIFACIO
        ("A727", -21.666, -49.734),  # LINS
        ("A763", -22.235, -49.965),  # MARILIA
        ("A726", -22.703, -47.623),  # PIRACICABA
        ("A747", -21.338, -48.113),  # PRADOPOLIS
        ("A707", -22.119, -51.408),  # PRESIDENTE PRUDENTE
        ("A718", -22.372, -50.974),  # RANCHARIA
        ("A766", -24.533, -47.864),  # REGISTRO
        ("A711", -21.980, -47.883),  # SAO CARLOS
        ("A740", -23.228, -45.416),  # SAO LUIZ DO PARAITINGA
        ("A715", -23.890, -47.999),  # SAO MIGUEL ARCANJO
        ("A771", -23.724, -46.677),  # SAO PAULO - INTERLAGOS
        ("A701", -23.496, -46.620),  # SAO PAULO - MIRANTE
        ("A770", -21.461, -47.579),  # SAO SIMAO
        ("A728", -23.041, -45.520),  # TAUBATE
        ("A768", -21.927, -50.490),  # TUPA
        ("A734", -21.319, -50.930),  # VALPARAISO
    ],
    "RJ": [
        ("A606", -22.975, -42.021),  # ARRAIAL DO CABO
        ("A604", -21.587, -41.958),  # CAMBUCI
        ("A607", -21.714, -41.343),  # CAMPOS DOS GOYTACAZES
        ("A620", -22.041, -41.051),  # CAMPOS DOS GOYTACAZES - SAO TOME
        ("A629", -21.938, -42.600),  # CARMO
        ("A608", -22.376, -41.811),  # MACAE
        ("A627", -22.867, -43.101),  # NITEROI
        ("A624", -22.334, -42.676),  # NOVA FRIBURGO - SALINAS
        ("A619", -23.223, -44.726),  # PARATY
        ("A637", -22.347, -43.417),  # PATY DO ALFERES - AVELAR
        ("A610", -22.464, -43.291),  # PICO DO COUTO
        ("A609", -22.451, -44.444),  # RESENDE
        ("A626", -22.653, -44.040),  # RIO CLARO
        ("A636", -22.939, -43.402),  # RIO DE JANEIRO - JACAREPAGUA
        ("A621", -22.861, -43.411),  # RIO DE JANEIRO - VILA MILITAR
        ("A602", -23.050, -43.595),  # RIO DE JANEIRO - MARAMBAIA
        ("A630", -21.950, -42.010),  # SANTA MARIA MADALENA
        ("A667", -22.871, -42.608),  # SAQUAREMA - SAMPAIO CORREIA
        ("A601", -22.757, -43.684),  # SEROPEDICA
        ("A659", -22.645, -42.415),  # SILVA JARDIM
        ("A618", -22.448, -42.986),  # TERESOPOLIS - PARQUE NACIONAL
        ("A625", -22.098, -43.208),  # TRES RIOS
        ("A611", -22.358, -43.695),  # VALENCA
    ],
    "MG": [
        ("A549", -15.751, -41.457),  # AGUAS VERMELHAS
        ("A534", -19.532, -41.090),  # AIMORES
        ("A508", -16.166, -40.687),  # ALMENARA
        ("A566", -16.848, -42.035),  # ARACUAI
        ("A505", -19.605, -46.949),  # ARAXA
        ("A565", -20.031, -46.008),  # BAMBUI
        ("A502", -21.228, -43.767),  # BARBACENA
        ("F501", -19.979, -43.958),  # BH - CERCADINHO
        ("A521", -19.883, -43.969),  # BH - PAMPULHA
        ("A572", -19.933, -43.952),  # BH - SANTO AGOSTINHO
        ("A544", -15.524, -46.435),  # BURITIS
        ("A519", -19.539, -49.518),  # CAMPINA VERDE
        ("A541", -17.705, -42.389),  # CAPELINHA
        ("A554", -19.735, -42.137),  # CARATINGA
        ("A557", -21.546, -43.261),  # CORONEL PACHECO
        ("A538", -18.747, -44.453),  # CURVELO
        ("A537", -18.231, -43.648),  # DIAMANTINA
        ("A564", -20.173, -44.874),  # DIVINOPOLIS
        ("A536", -19.481, -45.593),  # DORES DO INDAIA
        ("A543", -14.912, -42.808),  # ESPINOSA
        ("A535", -19.885, -44.416),  # FLORESTAL
        ("A524", -20.455, -45.453),  # FORMIGA
        ("A532", -18.830, -41.976),  # GOVERNADOR VALADARES
        ("A546", -17.561, -47.199),  # GUARDA-MOR
        ("A550", -16.575, -41.485),  # ITAOBIM
        ("A559", -15.448, -44.366),  # JANUARIA
        ("A553", -17.784, -46.119),  # JOAO PINHEIRO
        ("A518", -21.769, -43.364),  # JUIZ DE FORA
        ("A567", -21.680, -45.944),  # MACHADO
        ("A556", -20.263, -42.182),  # MANHUACU
        ("A540", -18.780, -40.986),  # MANTENA
        ("A531", -22.314, -45.373),  # MARIA DA FE
        ("A539", -15.085, -44.016),  # MOCAMBINHO
        ("A509", -22.861, -46.043),  # MONTE VERDE
        ("A506", -16.686, -43.843),  # MONTES CLAROS
        ("A517", -21.105, -42.375),  # MURIAE
        ("A563", -15.802, -43.296),  # NOVA PORTEIRINHA
        ("A570", -20.715, -44.864),  # OLIVEIRA
        ("A513", -20.556, -43.756),  # OURO BRANCO
        ("A571", -17.244, -46.881),  # PARACATU
        ("A516", -20.745, -46.633),  # PASSOS
        ("A523", -18.996, -46.985),  # PATROCINIO
        ("A545", -17.258, -44.835),  # PIRAPORA
        ("A560", -19.232, -44.964),  # POMPEU
        ("A551", -15.723, -42.435),  # RIO PARDO DE MINAS
        ("A525", -19.875, -47.434),  # SACRAMENTO
        ("A552", -16.160, -42.310),  # SALINAS
        ("A514", -21.106, -44.250),  # SAO JOAO DEL REI
        ("A522", -17.798, -40.250),  # SERRA DOS AIMORES
        ("A569", -19.455, -44.173),  # SETE LAGOAS
        ("A527", -17.892, -41.515),  # TEOFILO OTONI
        ("A511", -19.573, -42.622),  # TIMOTEO
        ("A528", -18.200, -45.459),  # TRES MARIAS
        ("A568", -19.710, -47.961),  # UBERABA
        ("A507", -18.916, -48.255),  # UBERLANDIA
        ("A542", -16.554, -46.881),  # UNAI
        ("A515", -21.566, -45.404),  # VARGINHA
        ("A510", -20.762, -42.863),  # VICOSA
        ("A520", -19.985, -48.151),  # CONCEICAO DAS ALAGOAS
    ],
    "RS": [
        ("B828", -31.874, -54.119),  # ACEGUA
        ("A826", -29.709, -55.525),  # ALEGRETE
        ("A827", -31.347, -54.013),  # BAGE
        ("B827", -31.305, -54.013),  # BAGE - CENTRO
        ("A840", -29.164, -51.534),  # BENTO GONCALVES
        ("A812", -30.545, -53.466),  # CACAPAVA DO SUL
        ("B822", -30.018, -52.938),  # CACHOEIRA DO SUL
        ("A838", -30.808, -51.834),  # CAMAQUA
        ("A884", -29.674, -51.064),  # CAMPO BOM
        ("A879", -29.368, -50.827),  # CANELA
        ("A811", -31.403, -52.700),  # CANGUCU
        ("A887", -31.802, -52.407),  # CAPAO DO LEAO
        ("B818", -29.196, -51.186),  # CAXIAS DO SUL - AEROPORTO
        ("B817", -28.874, -50.975),  # CAXIAS DO SUL - CRIUVA
        ("B812", -29.965, -51.625),  # CHARQUEADAS
        ("A853", -28.603, -53.673),  # CRUZ ALTA
        ("B833", -28.625, -53.613),  # CRUZ ALTA - CENTRO
        ("A893", -30.543, -52.524),  # ENCRUZILHADA DO SUL
        ("A828", -27.657, -52.305),  # ERECHIM
        ("A854", -27.395, -53.429),  # FREDERICO WESTPHALEN
        ("B826", -32.012, -53.403),  # HERVAL
        ("A883", -28.653, -53.111),  # IBIRUBA
        ("B850", -29.155, -56.555),  # ITAQUI
        ("A836", -32.534, -53.375),  # JAGUARAO
        ("B813", -30.124, -52.050),  # MINAS DO LEAO
        ("B814", -29.758, -51.436),  # MONTENEGRO
        ("B846", -27.358, -52.778),  # NONOAI
        ("B825", -30.186, -51.178),  # PORTO ALEGRE - BELEM NOVO
        ("A801", -30.053, -51.174),  # PORTO ALEGRE - JARDIM BOTANICO
        ("A831", -30.368, -56.437),  # QUARAI
        ("A802", -32.078, -52.167),  # RIO GRANDE
        ("A813", -29.872, -52.381),  # RIO PARDO
        ("B819", -29.655, -50.618),  # ROLANTE
        ("B838", -29.096, -53.212),  # SALTO DO JACUI
        ("A803", -29.724, -53.720),  # SANTA MARIA
        ("B816", -29.479, -51.013),  # SANTA MARIA DO HERVAL
        ("B821", -30.858, -53.155),  # SANTANA DA BOA VISTA
        ("A833", -29.191, -54.885),  # SANTIAGO
        ("B844", -28.278, -54.273),  # SANTO ANGELO
        ("B839", -29.816, -50.533),  # SANTO ANTONIO DA PATRULHA
        ("A805", -27.854, -53.791),  # SANTO AUGUSTO
        ("A830", -28.650, -56.016),  # SAO BORJA
        ("B820", -29.442, -50.560),  # SAO FRANCISCO DE PAULA
        ("B831", -31.340, -52.010),  # SAO LOURENCO DO SUL
        ("A852", -28.417, -54.962),  # SAO LUIZ GONZAGA
        ("B811", -30.494, -51.663),  # SERTAO SANTANA
        ("B840", -29.398, -53.013),  # SOBRADINHO
        ("B830", -33.531, -53.350),  # SANTA VITORIA DO PALMAR
        ("B829", -32.835, -52.643),  # SANTA VITORIA - RESERVA TAIM
        ("A899", -33.742, -53.372),  # SANTA VITORIA - BARRA DO CHUI
        ("A882", -29.449, -51.823),  # TEUTONIA
        ("B841", -29.413, -49.803),  # TORRES - AEROPORTO
        ("A834", -30.010, -50.135),  # TRAMANDAI
        ("A886", -29.089, -53.826),  # TUPANCIRETA
    ],
}