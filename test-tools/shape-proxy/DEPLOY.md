# Deploying Shape-Proxy Updates

## Quick Deploy to gary@10.21.8.63

### 1. Push latest changes to your fork
```bash
# On your local machine
cd ~/repos/sv2-apps
git push origin feat/shape-proxy
```

### 2. Pull and build on deploy server
```bash
ssh -i ~/.ssh/id_rsa gary@10.21.8.63

# Clone if first time (adjust path as needed)
cd ~
git clone https://github.com/YOUR_FORK/stratum-v2.git sv2-apps
cd sv2-apps
git checkout feat/shape-proxy

# Or pull if already exists
cd ~/sv2-apps  # or wherever it's located
git pull origin feat/shape-proxy

# Build release binary
cargo build --release --manifest-path test-tools/shape-proxy/Cargo.toml

# Binary will be at:
# ~/sv2-apps/target/release/shape-proxy
```

### 3. Update the running instances

**Option A: Via pool-boy UI (Recommended)**
1. In the pool-boy TUI, go to each slot (Slot 3, Slot 4, etc.)
2. Click "Stop" under the "Test Load (Shape Proxy)" section
3. Wait for it to stop
4. Click "Start Test Load"
5. The new binary will be used (pool-boy detects and uses the latest)

**Option B: Manual restart**
```bash
ssh -i ~/.ssh/id_rsa gary@10.21.8.63

# Find running shape-proxy processes
ps aux | grep shape-proxy

# Kill old processes (get PIDs from above)
kill <PID1> <PID2> ...

# pool-boy should auto-restart them, or use the UI
```

### 4. Verify the update

Check the API to confirm the new default profile:
```bash
curl http://10.21.8.63:8080/status | jq '.channels[0].profile'

# Should show:
# {
#   "type": "track",
#   "description": "1.0× supply"
# }
```

If you see `"type": "hold"` instead, the old binary is still running.

---

## Troubleshooting

### Old binary still running
```bash
# Check which binary is running
ssh -i ~/.ssh/id_rsa gary@10.21.8.63
ps aux | grep shape-proxy | grep -v grep

# Compare timestamps
ls -l ~/sv2-apps/target/release/shape-proxy
stat /proc/<PID>/exe

# If old, force restart via pool-boy UI or kill + restart
```

### Build fails
```bash
# Clean and rebuild
cd ~/sv2-apps
cargo clean
cargo build --release --manifest-path test-tools/shape-proxy/Cargo.toml
```

### API returns old profile format
- Old binary is still running
- Or config file has a profile preset (unlikely for shape-proxy)
- Check logs: `tail ~/.deploy-manager/slot3/logs/shape-proxy.log`

---

## Post-Deploy Verification Checklist

1. ✅ API shows `"type": "track"` as default profile
2. ✅ `supply_spm ≈ target_spm ≈ forwarded_spm` (normal smoothing)
3. ✅ Test buttons in pool-boy UI work:
   - Click "Drop 50%" → profile changes to `"type": "step"`
   - Wait 10 minutes → should auto-complete (if using frontend timer)
   - Or click "Cancel Test" → reverts to `"type": "track"`
4. ✅ Pool sees the test pattern (check pool logs or monitoring)

---

## Rollback (If Issues)

```bash
ssh -i ~/.ssh/id_rsa gary@10.21.8.63
cd ~/sv2-apps
git checkout <previous-commit>
cargo build --release --manifest-path test-tools/shape-proxy/Cargo.toml

# Restart via pool-boy UI
```
