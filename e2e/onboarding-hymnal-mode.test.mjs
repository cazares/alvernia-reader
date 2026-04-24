import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const APP_ROOT = path.resolve(new URL("..", import.meta.url).pathname);
const SOURCE = fs.readFileSync(path.join(APP_ROOT, "PdfReaderApp.tsx"), "utf8");

test("onboarding modal exists with required Spanish strings", () => {
  assert.match(SOURCE, /Código del coro/);
  assert.match(SOURCE, /Ingresa tu número autorizado para entrar al modo principal\./);
  assert.match(SOURCE, /Continuar como himnario/);
});

test("standard mode uses explicit choir codes and no city or device gate", () => {
  assert.match(SOURCE, /const CHOIR_STANDARD_ACCESS = new Map/);
  assert.match(SOURCE, /const normalizeAccessCode = \(value: string\): string =>/);
  assert.match(SOURCE, /const standardLockedRef = useRef\(false\);/);
  assert.match(SOURCE, /\["83078840", "Hector y Adrian"\]/);
  assert.match(SOURCE, /\["8303130470", "Braulio \(Original\)"\]/);
  const forbiddenCode = "830" + "7343376";
  assert.equal(SOURCE.includes(forbiddenCode), false);
  assert.doesNotMatch(SOURCE, /DEVICE_ALLOWLIST/);
  assert.doesNotMatch(SOURCE, /isAllowlistedStandardDevice/);
  assert.doesNotMatch(SOURCE, /Platform\.isPad/);
  assert.doesNotMatch(SOURCE, /isDelRioMatch/);
  assert.doesNotMatch(SOURCE, /setOnboardingState/);
  assert.doesNotMatch(SOURCE, /setOnboardingCity/);
});

test("choir-code devices stay pinned to standard mode", () => {
  assert.match(SOURCE, /standardLockedRef\.current = storedMode === "standard" && !!storedAccessName;/);
  assert.match(SOURCE, /await AsyncStorage\.multiSet\(\[\s+\[STORAGE_KEYS\.onboardingComplete, "1"\],\s+\[STORAGE_KEYS\.standardAccessName, name\],\s+\[STORAGE_KEYS\.mode, "standard"\]/s);
  assert.match(SOURCE, /if \(\s*standardLockedRef\.current && \(incomingMode !== "standard" \|\| incomingBookId !== "standard"\)\)\s*{\s*return;\s*}/s);
});

test("director mode cannot be unlocked by the internal session code", () => {
  assert.match(SOURCE, /const DIRECTOR_ACCESS_CODES = new Set\(\["8303130470", "8307197000"\]\);/);
  assert.match(SOURCE, /if \(!DIRECTOR_ACCESS_CODES\.has\(normalizeAccessCode\(codeInput\)\)\)/);
  assert.doesNotMatch(SOURCE, /DIRECTOR_ACCESS_CODES = new Set\(\[[^\]]*"1234"/);
});

test("followers can reach the in-app reset flow from the third refresh press", () => {
  assert.match(SOURCE, /syncRole === "follower" && !searchVisible && !onboardingVisible/);
  assert.match(SOURCE, /handleReconnectPress/);
  assert.match(SOURCE, /now - t <= 25_000/);
  assert.match(SOURCE, /reconnectPressesRef\.current\.length >= 3/);
  assert.match(SOURCE, /confirmResetApp\("reconnect"\)/);
  assert.match(SOURCE, /Si todavia no conecta, podemos restablecer la app ahora mismo\./);
  assert.match(SOURCE, /Esto vuelve a empezar la conexion sin borrar cantos ni ajustes\./);
  assert.match(SOURCE, /Verificando Bluetooth, Wi-Fi local y la conexión con el director\./);
  assert.doesNotMatch(SOURCE, /¿Quieres reiniciar la app\?/);
  assert.doesNotMatch(SOURCE, /Reinicia manualmente/);
});

test("the reader keeps the screen awake while open", () => {
  assert.match(SOURCE, /import \{ useKeepAwake \} from "expo-keep-awake"/);
  assert.match(SOURCE, /useKeepAwake\("signovivo-reader"\)/);
});

test("non-standard build removes the IR A LIBRO section entirely", () => {
  assert.doesNotMatch(SOURCE, /IR A LIBRO/);
  assert.doesNotMatch(SOURCE, /styles\\.bookList/);
  assert.doesNotMatch(SOURCE, /switchBook\\(id\\)/);
});

test("reset code 744668486 performs a soft app reset before normal navigation", () => {
  assert.match(SOURCE, /trimmed === \"744668486\"/);
  assert.match(SOURCE, /performSoftAppReset/);
  assert.match(SOURCE, /resetNearbyDirectorSync/);
  assert.doesNotMatch(SOURCE, /trimmed === \"744668486\"[\s\S]{0,250}clearAllBookState/);
});

test("song modal uses IR A CANTO copy and full 10 digit choir codes", () => {
  assert.match(SOURCE, /SONG_MODAL_INPUT_MAX_LENGTH = 10/);
  assert.match(SOURCE, />IR A CANTO</);
  assert.match(SOURCE, /placeholder=\"Número de canto\"/);
  assert.match(SOURCE, /♪ Abrir Canto ♪/);
  assert.match(SOURCE, /\[null, \"0\", \"⌫\"\]/);
  assert.match(SOURCE, /songModalBackdrop/);
  assert.match(SOURCE, /backKey/);
  assert.doesNotMatch(SOURCE, /IR A CANCION/);
  assert.doesNotMatch(SOURCE, /♪\s+Go/);
});

test("city onboarding storage failures stay inside the submit handler", () => {
  assert.match(SOURCE, /await AsyncStorage\.multiSet\(\[/);
  assert.match(SOURCE, /catch \{\n\s+Alert\.alert\(/);
  assert.match(SOURCE, /onboardingSubmittingRef\.current = false/);
});
