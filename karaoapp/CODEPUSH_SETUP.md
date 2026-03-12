# OTA Updates (CodePush-style) for Mixterious

This app is Expo-managed, so the modern "CodePush" path is **EAS Update + expo-updates**.
Legacy App Center CodePush is not used here.

## Scaffolded in this branch

- `expo-updates` dependency is installed.
- `app.config.js` now resolves `updates.url` from the EAS project id.
- OTA can be toggled with `EXPO_PUBLIC_OTA_ENABLED`.
- Auto-check on launch/foreground can be toggled with `EXPO_PUBLIC_OTA_AUTO_CHECK`.
- Build profiles in `eas.json` are pinned to channels.
- Publish helpers are available through npm scripts and `scripts/publish-ota.sh`.
- Feedback tab now includes an **App Updates** card to check/apply downloaded OTA updates.

## Channel map

| EAS build profile | Channel |
| --- | --- |
| `development` | `development` |
| `simulatorRelease` | `preview` |
| `preview` | `preview` |
| `production` | `production` |

## One-time setup (manual when you are back)

1. Go to the app:
   ```bash
   cd karaoapp
   ```
2. Authenticate:
   ```bash
   npx eas login
   ```
3. Initialize/link EAS project if needed:
   ```bash
   npx eas project:init
   ```
4. Add project id to env (copy from EAS output):
   ```bash
   EXPO_PUBLIC_EAS_PROJECT_ID=<your-project-id>
   ```
5. Optional sanity configure:
   ```bash
   npm run ota:configure
   ```
6. Validate OTA config:
   ```bash
   npm run ota:doctor
   # For CI/release gates:
   npm run ota:doctor:strict
   ```

## Build binaries tied to channels

First build each binary once so installed apps are channel-aware:

```bash
npx eas build --profile preview --platform ios
npx eas build --profile production --platform ios
```

## Publish OTA updates

```bash
npm run ota:doctor:strict
npm run ota:publish:preview
npm run ota:publish:production
```

Optional explicit message:

```bash
bash ./scripts/publish-ota.sh production ios "hotfix: improve upload retry"
```

## Inspect updates

```bash
npx eas update:list --branch production --platform ios --limit 10
npx eas update:view <update-group-id>
```

## Rollback options

Roll back to a previous update group:

```bash
npx eas update:republish --group <update-group-id> --branch production
```

Roll back to embedded bundle:

```bash
npx eas update:roll-back-to-embedded --branch production
```
