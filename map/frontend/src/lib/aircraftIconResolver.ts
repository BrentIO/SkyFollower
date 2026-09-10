// Picks a silhouette shape key for an aircraft, in the style established
// ADS-B viewers use: an exact ICAO type designator first, then the ICAO
// Doc 8643 description code (with wake-turbulence category to split the
// broad classes), then a plain fallback.
//
// Shape keys index AIRCRAFT_SHAPES (aircraftShapes.generated.ts), which is
// generated from the vendored SVGs (src/assets/aircraft-shapes/*.svg,
// GPL-3.0). The tables here are our own, built from the available shape
// set plus ICAO Doc 8643 -- no data copied from another project.

import type { AircraftInfo } from "../api/types";
import { AIRCRAFT_SHAPES } from "./aircraftShapes.generated";

export const FALLBACK_SHAPE = "UNIDENTIFIED";

// ICAO type designators we have no dedicated silhouette for -> the nearest
// available one. Keyed by the exact designator; only consulted when
// AIRCRAFT_SHAPES has no entry of its own for that designator. Exported for
// the test that asserts every target is a real shape key.
export const TYPE_ALIASES: Record<string, string> = {
  // Airbus
  A319: "A320", A310: "A310", A342: "A342",
  // Boeing narrowbody / MAX / NG
  B37M: "B38M", B38M: "B38M", B39M: "B39M", B3XM: "B38M",
  B736: "B735", B37: "B733",
  // Boeing widebody / freighter
  B77F: "B77W", B77L: "B77L", B762: "B762", B763: "B763", B764: "B764",
  B788: "B788", B789: "B789", B78X: "B78X",
  // Embraer E-Jets / ERJ
  E170: "E170", E175: "E170", E75L: "E170", E75S: "E170",
  E190: "E195", E290: "E195", E195: "E195", E295: "E195",
  E135: "E170", E145: "E170", E45X: "E170",
  // Bombardier CRJ / CSeries / Dash 8
  CRJ1: "CRJ2", CRJ2: "CRJ2", CRJ7: "CRJ7", CRJ9: "CRJ9", CRJX: "CRJX",
  BCS1: "BCS1", BCS3: "BCS3", A221: "BCS1", A223: "BCS3",
  DH8A: "DH8C", DH8B: "DH8C", DH8C: "DH8C", DH8D: "DH8D",
  // McDonnell Douglas
  MD82: "B712", MD83: "B712", MD87: "B712", MD88: "B712", MD90: "B712",
  MD11: "MD11", DC10: "DC10", DC93: "B712", B712: "B712",
  // ATR / regional turboprops
  AT43: "AT45", AT44: "AT45", AT45: "AT45", AT46: "AT45",
  AT72: "AT75", AT75: "AT75", AT76: "AT75",
  SF34: "SF34", SB20: "SF34", J31: "J328", J32: "J328", J41: "J328",
  SW4: "D328", D328: "D328", D228: "D228", B190: "B190", JS41: "J328",
  E120: "SF34",
  // BAe 146 / RJ / Avro
  B461: "RJ85", B462: "RJ85", B463: "RJ85", RJ1H: "RJ85", RJ70: "RJ85", RJ85: "RJ85",
  // Business jets
  C25A: "C25B", C25B: "C25B", C25C: "C25B", C25M: "C25B", C500: "C25B",
  C510: "C25B", C525: "C25B", C550: "C25B", C560: "C25B", C56X: "C25B",
  C650: "C25B", C680: "C25B", C68A: "C25B", C700: "C750", C750: "C750",
  E50P: "C25B", E55P: "C25B", E545: "C25B", E550: "C25B",
  LJ31: "LJ35", LJ35: "LJ35", LJ40: "LJ35", LJ45: "LJ35", LJ55: "LJ35",
  LJ60: "LJ35", LJ70: "LJ35", LJ75: "LJ35",
  H25B: "C25B", H25C: "C25B", HDJT: "C25B",
  GLF2: "GLF6", GLF3: "GLF6", GLF4: "GLF6", GLF5: "GLF6", GLF6: "GLF6",
  G150: "LJ35", GALX: "GLF6", G280: "GLF6",
  GLEX: "GL5T", GL5T: "GL5T", GL7T: "GL5T", GLF: "GLF6",
  F2TH: "FA7X", FA50: "FA7X", FA7X: "FA7X", FA8X: "FA7X", F900: "FA7X", F900EX: "FA7X",
  PC24: "C25B",
  // Light GA / pistons / turboprops
  C82R: "C172", C82S: "C172", C72R: "C172", C152: "C172", C162: "C172",
  C170: "C172", C177: "C172", C182: "C172", C206: "C208", C210: "C172",
  P28A: "P28A", P28B: "P28A", P28R: "P28A", P28T: "P28A", PA24: "P28A",
  PA28: "P28A", PA32: "P28A", PA34: "DA42", PA44: "DA42", PA46: "PA46",
  DA40: "SR22", DA42: "DA42", DA62: "DA42", SR20: "SR22", SR22: "SR22",
  BE33: "C172", BE35: "C172", BE36: "C172", BE58: "DA42", BE55: "DA42",
  BE20: "B350", B350: "B350", BE9L: "B350", C90: "B350", C90A: "B350",
  PC12: "PC12", PC6T: "PC6T", TBM7: "PC12", TBM8: "PC12", TBM9: "PC12",
  M20P: "C172", M20T: "C172", C25: "C172",
  // Helicopters
  A109: "EC35", A119: "EC35", A139: "EC45", A169: "EC45", A189: "EC45",
  AS50: "GAZL", AS55: "GAZL", AS65: "AS65", AS32: "AS32", AS3B: "AS32",
  EC20: "EC20", EC25: "AS32", EC30: "GAZL", EC35: "EC35", EC45: "EC45",
  EC55: "AS65", EC75: "AS32", H120: "EC20", H125: "GAZL", H130: "GAZL",
  H135: "EC35", H145: "EC45", H155: "AS65", H160: "AS65", H175: "AS32", H500: "GAZL",
  B06: "GAZL", B06T: "GAZL", B407: "EC35", B412: "S61", B429: "EC35",
  B430: "S61", B47G: "GAZL", B505: "GAZL", R22: "R44", R44: "R44", R66: "R44",
  S76: "AS65", S92: "S61", S61: "S61", S64: "H47", H60: "H60", UH60: "H60",
  EH10: "NH90", NH90: "NH90", LYNX: "LYNX", GAZL: "GAZL", MI8: "MI24",
  MI17: "MI24", MI24: "MI24", MI26: "H47", CH47: "H47", H47: "H47",
  // Military fixed-wing
  F15: "F15", F16: "F16", F18: "F18H", F18H: "F18H", F18S: "F18S", F22: "F22", F35: "F35",
  EUFI: "EUFI", TYP: "EUFI", RFAL: "RFAL", GR4: "TORSLOW", TOR: "TORSLOW",
  TORN: "TORSLOW", F111: "TORSLOW", B1: "B1SLOW", B1B: "B1SLOW", TU160: "B1SLOW",
  A10: "A10", A4: "A4", HAWK: "HAWK", HUNT: "HUNT", T38: "T38", M326: "M326",
  L159: "L159", AJET: "AJET", MIR2: "MIRA", MIRA: "MIRA", MRF1: "MRF1",
  // Military transports / patrol / tankers
  C130: "C130", C30J: "C130", L100: "C130", C160: "C160", A400: "A400",
  C17: "C17", C5: "C5M", C5M: "C5M", A124: "A124", AN12: "AN12", AN24: "AN26",
  AN26: "AN26", AN72: "AN26", IL76: "IL76", IL78: "IL76", C295: "C295",
  CN35: "CN35", C27J: "C295", K35R: "K35E", K35E: "K35E", KC46: "KC46",
  KC2: "KC2", KC30: "A332", A310MRTT: "A310", RC135: "R135", R135: "R135",
  E3: "E3TF", E3CF: "E3CF", E3TF: "E3TF", E7: "E737", E737: "E737",
  E8: "E8", P3: "P3", P8: "P8", P8A: "P8", NIM: "P3", B29: "B29", LANC: "B29",
  // VLA / gliders / balloons / gyro / UAV
  GLID: "GYRO", DISC: "GYRO", DG40: "GYRO", AS21: "AS21", SF25: "SF25",
  BALL: "BALL", SHIP: "BALL", GYRO: "GYRO", GYR: "GYRO",
  RQ4: "Q4", Q4: "Q4", MQ9: "Q4", RPAS: "Q4", UAV: "Q4",
};

// Description code (with an optional "-<WTC>" suffix) -> representative
// shape. WTC splits the broad jet/turboprop classes by size. Consulted
// after the type designator, before the plain fallback.
export const DESCRIPTION_SHAPES: Record<string, string> = {
  "H": "H60",
  "G": "GYRO",
  "T": "V22SLOW",

  "L1P": "C172", "A1P": "C172", "S1P": "C172",
  "L1T": "PC12", "A1T": "PC12",
  "L1J": "F16",

  "L2P": "DA42", "A2P": "DA42",
  "L2T": "AT45", "L2T-M": "AT75", "L2T-H": "AT75", "A2T": "AT45",

  "L2J": "A320", "L2J-L": "CRJ2", "L2J-M": "A320", "L2J-H": "B772", "L2J-J": "B772",
  "L3J": "MD11", "L3J-M": "B722", "L3J-H": "MD11",
  "L4T": "C130", "L4T-M": "C130", "L4T-H": "AN12",
  "L4J": "B744", "L4J-M": "B703", "L4J-H": "B744", "L4J-J": "B744",
  "L6J": "A388", "L8J": "A225",
};

function normalise(code: string | undefined | null): string {
  return (code ?? "").trim().toUpperCase();
}

/** The on-map size multiplier for a shape key (1 for an unknown key). */
export function shapeScale(shapeKey: string): number {
  return (AIRCRAFT_SHAPES[shapeKey] ?? AIRCRAFT_SHAPES[FALLBACK_SHAPE])?.scale ?? 1;
}

/** Shape key for one aircraft's metadata, always defined (FALLBACK_SHAPE
 * when nothing better is known). */
export function resolveAircraftShape(aircraft?: AircraftInfo | null): string {
  const type = normalise(aircraft?.type_designator);
  if (type) {
    if (AIRCRAFT_SHAPES[type]) return type;
    if (TYPE_ALIASES[type] && AIRCRAFT_SHAPES[TYPE_ALIASES[type]]) return TYPE_ALIASES[type];
  }

  const desc = normalise(aircraft?.description_code);
  const wtc = normalise(aircraft?.wake_turbulence_category);
  if (desc) {
    if (wtc) {
      const withWtc = DESCRIPTION_SHAPES[`${desc}-${wtc}`];
      if (withWtc && AIRCRAFT_SHAPES[withWtc]) return withWtc;
    }
    const exact = DESCRIPTION_SHAPES[desc];
    if (exact && AIRCRAFT_SHAPES[exact]) return exact;
    // Fall back on just the category letter (e.g. an unrecognised "L2C").
    const byCategory = DESCRIPTION_SHAPES[desc.charAt(0)];
    if (byCategory && AIRCRAFT_SHAPES[byCategory]) return byCategory;
  }

  return FALLBACK_SHAPE;
}
