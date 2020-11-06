Embulk::JavaPlugin.register_input(
  "postgresql_wal", "org.embulk.input.postgresql_wal.PostgresqlWalInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
