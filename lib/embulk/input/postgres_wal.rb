Embulk::JavaPlugin.register_input(
  "postgresql_wal", "org.embulk.input.postgresql_wal.PostgresWalInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
