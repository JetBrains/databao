import { Theme, Box, Text } from "@radix-ui/themes";
import { MultimodalTabs } from "@/components/MultimodalTabs/MultimodalTabs";
import styles from "./App.module.css";

function App() {
  const data = window.__DATA__;

  if (!data) {
    return (
      <Theme>
        <Box p="4">
          <Text color="red">No data available</Text>
        </Box>
      </Theme>
    );
  }

  return (
    <Theme>
      <div className={styles.appContainer}>
        <MultimodalTabs data={data} />
      </div>
    </Theme>
  );
}

export default App;
