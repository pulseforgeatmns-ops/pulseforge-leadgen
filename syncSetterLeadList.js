
// Export for cron route
module.exports = {
  run: async () => {
    console.log('[sync_setter] Starting sync...');
    await syncNewProspects();
    console.log('[sync_setter] Done.');
  }
};
