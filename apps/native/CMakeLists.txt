include(FetchContent)

set(CMAKE_AUTOMOC ON)
set(CMAKE_AUTORCC ON)
set(CMAKE_AUTOUIC ON)

find_package(Qt6 6.8 REQUIRED COMPONENTS Charts Network Test WebSockets Widgets)
set(protobuf_MODULE_COMPATIBLE TRUE)
find_package(Protobuf CONFIG REQUIRED)
find_package(unofficial-sodium CONFIG QUIET)
if(TARGET unofficial-sodium::sodium)
  set(MONTLOK_SODIUM_TARGET unofficial-sodium::sodium)
else()
  find_package(PkgConfig REQUIRED)
  pkg_check_modules(SODIUM REQUIRED IMPORTED_TARGET libsodium)
  set(MONTLOK_SODIUM_TARGET PkgConfig::SODIUM)
endif()
find_package(Arrow CONFIG REQUIRED)

set(KDDockWidgets_QT6 ON CACHE BOOL "" FORCE)
set(KDDockWidgets_FRONTENDS qtwidgets CACHE STRING "" FORCE)
set(KDDockWidgets_EXAMPLES OFF CACHE BOOL "" FORCE)
set(KDDockWidgets_TESTS OFF CACHE BOOL "" FORCE)
set(KDDockWidgets_DEVELOPER_MODE OFF CACHE BOOL "" FORCE)
FetchContent_Declare(
  kddockwidgets
  URL https://github.com/KDAB/KDDockWidgets/archive/refs/tags/v2.4.1.tar.gz
  URL_HASH SHA256=5cd9495d9316cf6cc937bb328a7fd491c3248ef2469cfa42511f1039f6148aca
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)
FetchContent_MakeAvailable(kddockwidgets)

set(BUILD_WITH_QT6 ON CACHE BOOL "" FORCE)
set(BUILD_TEST_APPLICATION OFF CACHE BOOL "" FORCE)
set(BUILD_TRANSLATIONS OFF CACHE BOOL "" FORCE)
set(BUILD_TESTING OFF CACHE BOOL "" FORCE)
FetchContent_Declare(
  qtkeychain
  URL https://github.com/frankosterfeld/qtkeychain/archive/refs/tags/0.17.0.tar.gz
  URL_HASH SHA256=3b85c3929034b0a99da777130c34d99f006fcd3a9d56564159399a33fee0e504
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)
FetchContent_MakeAvailable(qtkeychain)

set(MONTLOK_PROTO ${CMAKE_SOURCE_DIR}/packages/contracts/proto/montlok/v2/terminal.proto)
protobuf_generate_cpp(MONTLOK_PROTO_SRCS MONTLOK_PROTO_HDRS ${MONTLOK_PROTO})

qt_add_executable(montlok-terminal-native
  src/main.cpp
  src/ApiWorkbench.cpp
  src/ArrowTableModel.cpp
  src/CommandPalette.cpp
  src/DeviceIdentity.cpp
  src/EventBlotterModel.cpp
  src/EventInspector.cpp
  src/GatewayClient.cpp
  src/MainWindow.cpp
  src/RunOverview.cpp
  src/StrategyControlPanel.cpp
  src/TerminalContext.cpp
  ${MONTLOK_PROTO_SRCS}
  ${MONTLOK_PROTO_HDRS}
)

target_include_directories(montlok-terminal-native PRIVATE src ${CMAKE_CURRENT_BINARY_DIR})
target_compile_definitions(montlok-terminal-native PRIVATE
  MONTLOK_VERSION="${PROJECT_VERSION}"
  QT_NO_CAST_FROM_ASCII
  QT_NO_CAST_TO_ASCII
  QT_NO_KEYWORDS
)
target_link_libraries(montlok-terminal-native PRIVATE
  KDAB::kddockwidgets
  ${MONTLOK_SODIUM_TARGET}
  protobuf::libprotobuf
  qt6keychain
  Qt6::Charts
  Qt6::Network
  Qt6::WebSockets
  Qt6::Widgets
)

if(Arrow_FOUND)
  target_compile_definitions(montlok-terminal-native PRIVATE MONTLOK_HAS_ARROW=1)
  if(TARGET Arrow::arrow_shared)
    target_link_libraries(montlok-terminal-native PRIVATE Arrow::arrow_shared)
  else()
    target_link_libraries(montlok-terminal-native PRIVATE Arrow::arrow_static)
  endif()
endif()

if(APPLE)
  set_target_properties(montlok-terminal-native PROPERTIES
    MACOSX_BUNDLE TRUE
    MACOSX_BUNDLE_GUI_IDENTIFIER com.montlok.terminal
    MACOSX_BUNDLE_BUNDLE_NAME "Montlok Terminal"
  )
elseif(WIN32)
  set_target_properties(montlok-terminal-native PROPERTIES WIN32_EXECUTABLE TRUE)
endif()

install(TARGETS montlok-terminal-native
  BUNDLE DESTINATION .
  RUNTIME DESTINATION bin
)

include(CTest)
if(MONTLOK_BUILD_TESTS)
  qt_add_executable(montlok-native-tests
    tests/TerminalContextTest.cpp
    src/TerminalContext.cpp
    src/EventBlotterModel.cpp
  )
  target_include_directories(montlok-native-tests PRIVATE src)
  target_compile_definitions(montlok-native-tests PRIVATE QT_NO_KEYWORDS QT_NO_CAST_FROM_ASCII QT_NO_CAST_TO_ASCII)
  target_link_libraries(montlok-native-tests PRIVATE Qt6::Test Qt6::Widgets)
  add_test(NAME montlok-native-tests COMMAND montlok-native-tests)
endif()
