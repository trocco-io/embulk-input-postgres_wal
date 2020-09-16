Embulk::JavaPlugin.register_input(
  "postgres_wal", "org.embulk.input.postgres_wal.PostgresWalInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
